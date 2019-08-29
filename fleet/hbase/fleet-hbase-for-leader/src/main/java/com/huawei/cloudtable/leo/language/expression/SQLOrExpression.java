package com.huawei.cloudtable.leo.language.expression;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.statement.SQLExpression;
import com.huawei.cloudtable.leo.language.statement.SQLExpressionVisitor;
import com.huawei.cloudtable.leo.language.annotation.Required;

public final class SQLOrExpression extends SQLExpression.Priority7 {

  public SQLOrExpression(
      @Required final SQLExpression.Priority7 parameter1,
      @Required final SQLKeywords.OR or,
      @Required final SQLExpression.Priority7 parameter2
      ) {
    if (parameter1 == null) {
      throw new IllegalArgumentException("Argument [parameter1] is null.");
    }
    if (or == null) {
      throw new IllegalArgumentException("Argument [or] is null.");
    }
    if (parameter2 == null) {
      throw new IllegalArgumentException("Argument [parameter2] is null.");
    }
    this.parameter1 = parameter1;
    this.or = or;
    this.parameter2 = parameter2;
  }

  private final SQLExpression parameter1;

  private final SQLKeywords.OR or;

  private final SQLExpression parameter2;

  public SQLExpression getParameter1() {
    return this.parameter1;
  }

  public SQLExpression getParameter2() {
    return this.parameter2;
  }

  @Override
  public <TVisitorResult, TVisitorParameter> TVisitorResult accept(
      final SQLExpressionVisitor<TVisitorResult, TVisitorParameter> visitor,
      final TVisitorParameter visitorParameter
  ) {
    return visitor.visit(this, visitorParameter);
  }

  @Override
  public void toString(final StringBuilder stringBuilder) {
    this.parameter1.toString(stringBuilder);
    stringBuilder.append(' ');
    this.or.toString(stringBuilder);
    stringBuilder.append(' ');
    this.parameter2.toString(stringBuilder);
  }

}
