from io import BytesIO
from matplotlib.figure import Figure
from deephaven.plugin.object import Exporter, ObjectType

NAME = "matplotlib.figure.Figure"

print("Hello from plugin!")

class FigureType(ObjectType):

    def __init__(self):
        self.handle = None

    @property
    def name(self) -> str:
        return NAME

    def is_type(self, object) -> bool:
        return isinstance(object, Figure)

    def to_bytes(self, exporter: Exporter, figure: Figure) -> bytes:
        from deephaven import listen
        from deephaven.TableTools import emptyTable
        import jpy

        print("to_bytes!!!")

        LivenessScope = jpy.get_type('io.deephaven.engine.liveness.LivenessScope')
        LivenessScopeStack = jpy.get_type('io.deephaven.engine.liveness.LivenessScopeStack')
        liveness_scope = LivenessScope(True)
        LivenessScopeStack.push(liveness_scope)

        table = jpy.get_type('io.deephaven.engine.table.impl.util.KeyedArrayBackedMutableTable').make(emptyTable(0).updateView('MsgId=""', 'Msg=""'), 'MsgId')

        def listener_function(update):
            print(f"FUNCTION LISTENER: update={update}")

            added_iterator = update.added.iterator()

            while added_iterator.hasNext():
                index = added_iterator.nextLong()
                msgId = table.getColumnSource("MsgId").get(index)
                msg = table.getColumnSource("Msg").get(index)
                print(f"\tADDED VALUES: MsgId={msgId} Msg={msg}")

            modified_iterator = update.modified.iterator()

            while modified_iterator.hasNext():
                index = modified_iterator.nextLong()
                msgId = table.getColumnSource("MsgId").get(index)
                msg = table.getColumnSource("Msg").get(index)
                print(f"\tMODIFIED VALUES: MsgId={msgId} Msg={msg}")

        listen(table, listener_function)
        exporter.reference(table)
        exporter.reference(liveness_scope)
        LivenessScopeStack.pop(liveness_scope)
        buf = BytesIO()
        figure.savefig(buf, format='PNG', dpi=144)
        return buf.getvalue()
