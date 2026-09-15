package tech.streamfusion.planner;

import java.math.BigDecimal;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExprCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/** Flink's temporal expression code, called once per argument batch through the scalar bridge. */
public final class TemporalFunction extends ScalarFunction {
  private static final long serialVersionUID = 1L;

  public interface Evaluator extends Function {
    Object eval(RowData input) throws Exception;

    void open(FunctionContext context) throws Exception;

    void close() throws Exception;
  }

  private final GeneratedFunction<Evaluator> generated;
  private final LogicalType[] argumentTypes;
  private transient Evaluator evaluator;
  private transient GenericRowData input;

  TemporalFunction(RexNode expression, LogicalType[] argumentTypes, ReadableConfig config) {
    this.argumentTypes = argumentTypes;
    var context = new Context(config);
    var generator = new ExprCodeGenerator(context, false);
    generator.bindInput(RowType.of(argumentTypes), "input", scala.Option.empty());
    var result = generator.generateExpression(expression);
    String className = "TemporalEvaluator" + context.getNameCounter().getAndIncrement();
    String code =
        "public final class "
            + className
            + " implements "
            + Evaluator.class.getCanonicalName()
            + " {\n"
            + context.reuseMemberCode()
            + "public "
            + className
            + "(Object[] references) throws Exception {\n"
            + context.reuseInitCode()
            + "}\n"
            + context.reuseConstructorCode(className)
            + "public void open("
            + FunctionContext.class.getCanonicalName()
            + " context) throws Exception {\n"
            + context.reuseOpenCode()
            + "}\n"
            + "public Object eval("
            + RowData.class.getCanonicalName()
            + " input) throws Exception {\n"
            + context.reusePerRecordCode()
            + context.reuseLocalVariableCode(context.reuseLocalVariableCode$default$1())
            + context.reuseInputUnboxingCode()
            + result.code()
            + "\nif ("
            + result.nullTerm()
            + ") { return null; }\nreturn "
            + result.resultTerm()
            + ";\n}\n"
            + "public void close() throws Exception {\n"
            + context.reuseCloseCode()
            + "}\n"
            + context.reuseInnerClassDefinitionCode()
            + "}\n";
    Object[] references =
        scala.collection.JavaConverters.seqAsJavaList(context.references()).toArray();
    generated = new GeneratedFunction<>(className, code, references, config);
    generated.compile(TemporalFunction.class.getClassLoader());
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    evaluator = generated.newInstance(TemporalFunction.class.getClassLoader());
    evaluator.open(context);
    input = new GenericRowData(argumentTypes.length);
  }

  public Object eval(Object... arguments) throws Exception {
    for (int i = 0; i < arguments.length; i++) {
      Object value = arguments[i];
      if (value instanceof String text) {
        value = StringData.fromString(text);
      } else if (value instanceof BigDecimal decimal) {
        DecimalType type = (DecimalType) argumentTypes[i];
        value = DecimalData.fromBigDecimal(decimal, type.getPrecision(), type.getScale());
      }
      input.setField(i, value);
    }
    Object result = evaluator.eval(input);
    return result instanceof DecimalData decimal ? decimal.toBigDecimal() : result;
  }

  @Override
  public void close() throws Exception {
    if (evaluator != null) {
      evaluator.close();
      evaluator = null;
    }
  }

  private static final class Context extends CodeGeneratorContext {
    Context(ReadableConfig config) {
      super(config, TemporalFunction.class.getClassLoader());
    }

    @Override
    public String addReusableFunction(
        UserDefinedFunction function,
        Class<? extends FunctionContext> contextClass,
        scala.collection.Seq<String> contextArguments) {
      String name =
          addReusableObject(function, "temporalFunction", function.getClass().getCanonicalName());
      addReusableOpenStatement(name + ".open(context);");
      addReusableCloseStatement(name + ".close();");
      return name;
    }
  }
}
