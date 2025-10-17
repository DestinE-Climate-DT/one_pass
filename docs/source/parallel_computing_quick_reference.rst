Parallel Computing Quick Reference
===================================

This page provides a quick reference for using MPI and multiprocessing with OnePass.

.. important::
   **Critical Information:**
   
   - Only ``histogram`` and ``percentile`` support parallelization
   - MPI checkpoints require same process count for resume
   - Only rank 0 returns results when using MPI (others return ``None``)
   - Default is serial (``workers=1``); must explicitly enable parallelization

At a Glance
-----------

**Supported Statistics:**
- ``histogram`` ✓
- ``percentile`` ✓
- All others: Serial only

**Parallel Methods:**
- **MPI**: Multi-node distributed computing
- **Multiprocessing**: Single-node parallel processing
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
   
   # Use 4 worker processes
   opa = Opa(request, workers=4)
   result = opa.compute(data)

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

- :doc:`advanced_usage` - Detailed MPI and multiprocessing guide
- :doc:`troubleshooting` - Common issues and solutions
- :doc:`api_reference` - MPI module API documentation

