"""Reading a magmalake Iceberg table as a Daft DataFrame, two ways.

`FlightSource` goes over Arrow Flight, which crosses a process or a machine.
`IcebergLocalSource` calls a Mojo shared library in this process and hands Daft
the buffers where they already are. Same plan, same tickets, same DataFrame.
"""

from daft_flight.local import IcebergLocalSource
from daft_flight.source import FlightSource

__all__ = ["FlightSource", "IcebergLocalSource"]
