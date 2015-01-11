/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.el;

import org.apache.commons.el.BinaryOperatorExpression;
import org.apache.commons.el.BooleanLiteral;
import org.apache.commons.el.ComplexValue;
import org.apache.commons.el.ConditionalExpression;
import org.apache.commons.el.Expression;
import org.apache.commons.el.ExpressionString;
import org.apache.commons.el.FloatingPointLiteral;
import org.apache.commons.el.FunctionInvocation;
import org.apache.commons.el.IntegerLiteral;
import org.apache.commons.el.NamedValue;
import org.apache.commons.el.NullLiteral;
import org.apache.commons.el.StringLiteral;
import org.apache.commons.el.UnaryOperatorExpression;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.jsp.el.ELException;

public class TestELBasicSupport {

  @Test
  public void testRecordFunctions() throws Exception {
    ELEvaluator eval = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(eval);

    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Assert.assertTrue(eval.eval(variables, "${isIn('a', 'a,b,c,d')}", Boolean.class));
    Assert.assertTrue(eval.eval(variables, "${notIn('e', 'a,b,c,d')}", Boolean.class));
  }

  @Test
  public void testEvaluator() throws Exception {

    ELEvaluator eval = new ELEvaluator();
    Object result = eval.parseExpression("${isIn('a', 'a,b,c,d')}");

    if(result != null) {
      if (result instanceof String) {
        System.out.println(result);
      } else if (result instanceof Expression) {
        Expression e = (Expression) result;

        if( e instanceof BinaryOperatorExpression) {

        } else if (e instanceof BooleanLiteral) {

        } else if (e instanceof ComplexValue) {

        } else if (e instanceof ConditionalExpression) {

        } else if (e instanceof FunctionInvocation) {

        } else if (e instanceof FloatingPointLiteral) {

        } else if (e instanceof IntegerLiteral) {

        } else if (e instanceof NamedValue) {

        } else if (e instanceof NullLiteral) {

        } else if (e instanceof StringLiteral) {

        } else if (e instanceof UnaryOperatorExpression) {

        }

      } else if (result instanceof ExpressionString) {
        ExpressionString es = (ExpressionString) result;
        Object[] elements = es.getElements();
      }
    }
    System.out.println("Fin parsing : " + result.toString());
  }

  @Test
  public void test() throws ELException {
    ELEvaluator eval = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(eval);
    ELEvaluator.Variables variables = new ELEvaluator.Variables();

    Object result = eval.eval(variables, "${isIn('a', 'a,b,c,d')}");
    System.out.println("Fin parsing : " + result.toString());
  }

}
