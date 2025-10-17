from one_pass.mpi import MPI_LAUNCHED
from .initialize import initialise_no_mpi, initialise_mpi
from .update import update_mpi, update_no_mpi

if MPI_LAUNCHED:
    initialise = initialise_mpi
    update = update_mpi
else:
    initialise = initialise_no_mpi
    update = update_no_mpi

__all__ = [
    "initialise",
    "update",
]
