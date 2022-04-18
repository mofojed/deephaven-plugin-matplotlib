from __future__ import annotations

from deephaven.plugin import Registration
from deephaven.plugin.object \
    import Exporter, ObjectType, Reference

__version__ = "0.0.1.dev7"

# Class defines a Sin example object
# You can set the count of series, and the size each series will be
class Sin:
    def __init__(self, count = 1, size = 100) -> None:
        self._count = count
        self._size = size

    @property
    def count(self):
        return self._count

    @property
    def size(self):
        return self._size

class SinType(ObjectType):
    @property
    def name(self) -> str:
        return "deephaven.plugin.sin.Sin"

    def is_type(self, object) -> bool:
        return isinstance(object, Sin)

    def to_bytes(self, exporter: Exporter, sin: Sin) -> bytes:
        from deephaven.TableTools import emptyTable
        from deephaven import doLocked
        from deephaven import Plot
        import jpy

        print("SinType.to_bytes!!!")

        # We need to get the liveness scope so we can run table operations
        LivenessScope = jpy.get_type('io.deephaven.engine.liveness.LivenessScope')
        LivenessScopeStack = jpy.get_type('io.deephaven.engine.liveness.LivenessScopeStack')
        liveness_scope = LivenessScope(True)
        LivenessScopeStack.push(liveness_scope)

        series_count = sin.count

        def create_tables():
            # Create the table for inputs
            inputs = jpy.get_type('io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable').make(emptyTable(series_count).updateView('Series="" + i', "A=1.0d", "f=1.0d", "b=0.0d"), 'Series')
            exporter.reference(inputs)

            result = Plot
            for s in range(0, series_count):
                t = emptyTable(100).join(inputs.where("Series = `" + str(s) + "`")).updateView("x=i", "y=A*Math.sin(x*f) + b")
                exporter.reference(t)
                result = result.plot("Series" + str(s), t, "x", "y").show()

            exporter.reference(result)

        doLocked(create_tables)

        exporter.reference(liveness_scope)
        LivenessScopeStack.pop(liveness_scope)
        return str.encode("{ \"count\": " + str(sin.count) + ", \"size\": " + str(sin.size) + "}")


class MatplotlibRegistration(Registration):
    @classmethod
    def register_into(cls, callback: Registration.Callback) -> None:
        callback.register(SinType)
