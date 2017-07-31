package org.cloudgraph.hbase.filter;

import java.util.ArrayList;
import java.util.List;

import org.plasma.query.model.Expression;
import org.plasma.query.model.Property;
import org.plasma.query.model.Term;
import org.plasma.query.model.Where;

public class PredicateUtil {

	public boolean hasHeterogeneousDescendantProperties(Where where) {
		return hasHeterogeneousDescendantProperties(where.getExpressions().get(
				0));
	}

	public boolean hasHeterogeneousChildProperties(Where where) {
		return hasHeterogeneousChildProperties(where.getExpressions().get(0));
	}

	/**
	 * Returns true if the given expression has any immediate child
	 * property-expressions where the properties are heterogeneous i.e. more
	 * than one distinct property.
	 * 
	 * @param expression
	 *            the expression
	 * @return true if the given expression has any immediate child
	 *         property-expressions where the properties are heterogeneous.
	 */
	// FIXME: does not address paths
	public boolean hasHeterogeneousChildProperties(Expression expression) {
		String firstName = null;

		for (Term term : expression.getTerms())
			if (term.getExpression() != null) {
				Expression childExpr = term.getExpression();
				for (Term childTerm : childExpr.getTerms())
					if (childTerm.getProperty() != null) {
						Property childProperty = childTerm.getProperty();
						if (firstName == null) {
							firstName = childProperty.getName();
						} else {
							if (!firstName.equals(childProperty.getName()))
								return true;
						}
					}
			}

		return false;
	}

	public boolean hasHeterogeneousDescendantProperties(Expression expression) {
		String firstName = null;
		Property[] props = findProperties(expression);
		for (Property prop : props) {
			if (firstName == null) {
				firstName = prop.getName();
			} else {
				if (!firstName.equals(prop.getName()))
					return true;
			}
		}
		return false;
	}

	public Property[] findProperties(Expression expression) {
		List<Property> list = new ArrayList<Property>();
		collectProperties(expression, list);
		Property[] result = new Property[list.size()];
		list.toArray(result);
		return result;
	}

	public void collectProperties(Expression expression, List<Property> list) {
		for (Term term : expression.getTerms()) {
			if (term.getExpression() != null)
				collectProperties(term.getExpression(), list);
			else if (term.getProperty() != null)
				list.add(term.getProperty());
		}
	}

}
