Parallel Computing Quick Reference
===================================

This page provides a quick reference for using MPI and multiprocessing with OnePass.

.. important::
   **Critical Information:**

   - Only ``histogram`` and ``percentile`` support parallelization
   - MPI checkpoints require same process count for resume
   - Only rank 0 returns results when using MPI (others return ``None``)
   - Default is serial (``workers=1``); must explicitly enable parallelization
   - ``workers > 1`` (multiprocessing) requires ``checkpoint=True`` in the request
   - MPI and multiprocessing (``workers > 1``) cannot be used simultaneously

At a Glance
-----------

**Supported Statistics:**

- ``histogram`` ✓
- ``percentile`` ✓
- All others: Serial only

**Parallel Methods:**

- **MPI**: Multi-node distributed computing
- **Multiprocessing**: Single-node parallel processing using shared memory
- **Serial**: Default (workers=1, no MPI)

Quick Start: MPI
-----------------

1. **Install MPI**:

   .. code-block:: bash

      # Ubuntu/Debian
      sudo apt-get install libopenmpi-dev openmpi-bin
      pip install mpi4py

2. **Run your script**:

   .. code-block:: bash

      mpirun -n 4 python your_script.py

3. **No code changes needed!** MPI is auto-detected.

Quick Start: Multiprocessing
-----------------------------

.. code-block:: python

   from one_pass import Opa

   # Multiprocessing requires checkpoint=True
   request = {
       "stat": "histogram",  # or "percentile"
       "checkpoint": True,
       "checkpoint_filepath": "/path/to/checkpoints/",
       # ... other request keys ...
   }

   # Use 4 worker processes
   opa = Opa(request, workers=4)
   result = opa.compute(data)

.. note::

   ``workers > 1`` requires ``checkpoint=True`` in the request. This is because
   in MP mode each worker manages its own TDigest objects, which must be persisted
   to disk between calls (see :ref:`how-mp-works`). A ``ValueError`` is raised
   at ``Opa`` construction time if this constraint is not satisfied.

.. _how-mp-works:

How Multiprocessing Works
--------------------------

The MP implementation uses a **shared-memory strategy via** ``/dev/shm`` to overcome
the serialization bottleneck that would arise when passing large TDigest arrays between
processes through normal inter-process communication.

.. rubric:: Update phase (per ``compute()`` call)

At each call to ``opa.compute(data)``, the input data array is written as a NumPy
``.npy`` file to ``/dev/shm`` (Linux shared-memory filesystem) if it is available,
or to the system temporary directory otherwise. Worker processes then use
``numpy.memmap`` to read only their own assigned slice of grid points from that file —
no full-array copy is made inside each worker.

Each worker independently maintains its own TDigest objects for its slice of the grid,
saving them to a dedicated checkpoint file after each update:

.. code-block:: text

   checkpoint_{variable}_{stat_freq}_timestep_{time_step}_{output_freq}_{stat}.{worker_id:06d}-{n_workers:06d}.pkl

This per-worker checkpoint naming convention is shared with MPI ranks, ensuring
consistent checkpoint file management across all parallel modes.

The shared-memory data file is deleted as soon as all workers have finished their
update, keeping ``/dev/shm`` usage minimal.

.. rubric:: Finalization phase (when a statistics period completes)

When a statistics period is complete, memory-mapped result arrays are created in
``/dev/shm``. Workers load their TDigests from their own checkpoint files, compute
histograms or percentiles for their assigned grid points, and write the results
*directly* into the shared memory-mapped arrays at the correct column offset — no
per-worker result arrays are returned across process boundaries.

The main process then reads the consolidated result back from the memory-mapped files
and continues with normal output handling. Both the per-worker TDigest checkpoint files
and the temporary memory-mapped result files are cleaned up automatically.

.. rubric:: Fallback behaviour

If ``/dev/shm`` is not available (e.g. on macOS or certain HPC environments), all
temporary files fall back transparently to ``tempfile.gettempdir()``. The algorithm
is otherwise identical.

When to Use What
----------------

+----------------------+------------------+-------------------+
| Scenario             | Use              | Command/Code      |
+======================+==================+===================+
| Small grid (<100)    | Serial           | ``workers=1``     |
+----------------------+------------------+-------------------+
| Medium grid          | Multiprocessing  | ``workers=4``     |
| (100-10k points)     |                  |                   |
+----------------------+------------------+-------------------+
| Large grid (>10k)    | MPI              | ``mpirun -n N``   |
+----------------------+------------------+-------------------+
| Multi-node cluster   | MPI              | ``mpirun -n N``   |
+----------------------+------------------+-------------------+
| Single workstation   | Multiprocessing  | ``workers=N``     |
+----------------------+------------------+-------------------+

Code Examples
-------------

Check MPI Status
^^^^^^^^^^^^^^^^

.. code-block:: python

   from one_pass import MPI_LAUNCHED, MPI_RANK, MPI_SIZE

   if MPI_LAUNCHED:
       print(f"Running with MPI: Rank {MPI_RANK}/{MPI_SIZE}")
   else:
       print("Running in serial mode")

Handle MPI Output
^^^^^^^^^^^^^^^^^

.. code-block:: python

   result = opa.compute(data)

   # Only rank 0 has the result
   if result is not None:
       print("Computation complete!")
       result.to_netcdf("output.nc")

MPI with Checkpoints
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # First run
   # mpirun -n 4 python script.py
   opa = Opa(request)
   opa.compute(data)

   # Resume (must use same -n 4!)
   # mpirun -n 4 python script.py
   opa = Opa(request)  # Auto-loads checkpoint
   opa.compute(more_data)

Multiprocessing with Checkpoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   request = {
       "stat": "percentile",
       "checkpoint": True,            # required for workers > 1
       "checkpoint_filepath": "/tmp/opa_checkpoints/",
       "save": True,
       "save_filepath": "/tmp/opa_output/",
       # ... other request keys ...
   }

   opa = Opa(request, workers=4)

   # Process data in chunks (typical streaming usage)
   for chunk in data_stream:
       result = opa.compute(chunk)

   # result holds the final statistic when the period completes

Performance Tips
----------------

**MPI Best Practices:**

- Use process count that divides evenly into spatial grid size
- Don't use more processes than spatial points
- Resume with same process count when using checkpoints

**Multiprocessing Best Practices:**

- Use ``workers`` = number of CPU cores (or less)
- Avoid for very small or very large grids
- Good for medium-sized single-node jobs
- Point ``checkpoint_filepath`` to ``/dev/shm`` for maximum throughput: the per-worker
  TDigest checkpoint files will reside entirely in RAM, eliminating disk I/O during the
  update loop. OnePass does this automatically when no ``checkpoint_filepath`` is set,
  but you can also set it explicitly:

  .. code-block:: python

     request = {
         "stat": "histogram",
         "checkpoint": True,
         "checkpoint_filepath": "/dev/shm",   # RAM-backed, fastest option
         "save": True,
         "save_filepath": "/path/to/output/",
         # ...
     }
     opa = Opa(request, workers=4)

  .. note::
     ``/dev/shm`` is a RAM-backed tmpfs on Linux. Files stored there count against
     available RAM. Make sure the combined size of your per-worker checkpoint files
     fits comfortably in your system's available memory.

**General:**

- Process data in natural chunks (e.g., daily, 6-hourly)
- Avoid single-timestep processing
- Monitor memory usage for large grids

Common Issues
-------------

MPI not detected
^^^^^^^^^^^^^^^^

.. code-block:: bash

   # Check environment
   python -c "from one_pass import MPI_LAUNCHED; print(MPI_LAUNCHED)"

   # Should print True when run with mpirun
   mpirun -n 2 python -c "from one_pass import MPI_LAUNCHED; print(MPI_LAUNCHED)"

Results differ between MPI and serial
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Ensure same configuration
   # Verify same data
   # Check same number of processes for checkpoint resume

Checkpoint error after changing process count
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   # Delete checkpoints and restart
   rm checkpoint_*.pkl
   mpirun -n 8 python script.py  # New process count

``ValueError``: workers > 1 require checkpointing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Multiprocessing mode requires ``checkpoint=True`` in the request because each
worker persists its TDigest objects to a separate checkpoint file between calls.
Set ``checkpoint=True`` and provide a ``checkpoint_filepath`` in your request:

.. code-block:: python

   request = {
       "stat": "histogram",
       "checkpoint": True,                    # <-- required
       "checkpoint_filepath": "/dev/shm",     # or any writable directory
       # ...
   }
   opa = Opa(request, workers=4)

``RuntimeError``: cannot use MPI and multiprocessing simultaneously
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

MPI (launched via ``mpirun``/``mpiexec``) and the ``workers > 1`` multiprocessing
mode are mutually exclusive. Use one or the other:

- For multi-node parallelism: ``mpirun -n N python script.py`` with ``workers=1``
  (the default).
- For single-node parallelism: run without ``mpirun``, set ``workers=N``.

Performance Reference
---------------------

Typical Performance (50x50 grid, 24 timesteps):

- **Serial** (workers=1): ~10s
- **Multiprocessing** (workers=4): ~4s (2.5x speedup)
- **MPI** (4 processes): ~3s (3.3x speedup)

.. note::
   Actual performance varies by hardware, data size, and configuration.

See Also
--------

- :doc:`the_data_request` - Full request key reference, including checkpoint options
- :doc:`debugging` - Logging and internal diagnostics
