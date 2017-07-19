package org.cloudgraph.store.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.plasma.sdo.PlasmaChangeSummary;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.core.CoreDataObject;

/**
 * Comparator which imposes a commit ordering on created objects based on singular relations between the
 * types for the given source and target data objects. Singular relations across any number of
 * "hops" between the 2 types are detected and the "parent" types are ordered first. Where the types
 * are the same, singular relations may still exist from/to the same type forming a "recursion" and
 * these are detected by finding singular linkages between between the given data objects.    
 * Note: this comparator imposes orderings that are inconsistent with equals.
 */
public class CreatedCommitComparator extends CommitComparator {

	private static final long serialVersionUID = 1L;
    private static Log log = LogFactory.getFactory().getInstance(
    		CreatedCommitComparator.class);

	@Override
	public int compare(CoreDataObject source, CoreDataObject target) {
		
    	PlasmaType targetType = (PlasmaType)target.getType();
    	PlasmaType sourceType = (PlasmaType)source.getType();
        PlasmaChangeSummary changeSummary = (PlasmaChangeSummary)source.getDataGraph().getChangeSummary();
        int targetDepth = changeSummary.getPathDepth(target);
        int sourceDepth = changeSummary.getPathDepth(source);

        if (targetType.getQualifiedName() != sourceType.getQualifiedName()) {
    		if (log.isDebugEnabled())
    			log.debug("comparing types: "
                    + sourceType.toString() + " / " + targetType.toString());

    		if (isSingularRelation(source, target)) { // source is less than target
    			if (log.isDebugEnabled())
    				log.debug("(return -1) - singular relation from source: "
    	                + sourceType.toString() + " to target: " 
    						+ targetType.toString());
    	        return -1;
    		}
    		else if (isSingularRelation(target, source)) { // source is greater than target
    			if (log.isDebugEnabled())
    				log.debug("(return 1) - singular relation to target: "
    	                + targetType.toString() + " from source: " 
    						+ sourceType.toString());
    	        return 1;
    		}
    		else {
    			if (log.isDebugEnabled())
    				log.debug("(return 0)");
    			return 0;
    		}    		 
    	}
    	else {
    		if (log.isDebugEnabled())
    			log.debug("comparing data objects: "
                    + source.toString() + " / " + target.toString());
    		// give precedence to reference links, then to
    		// graph path depth
    		if (hasChildLink(source, target)) {
    			if (log.isDebugEnabled())
    				log.debug("(return 1) singular link from : "
    	                + source.toString() + " to: " + target.toString());
    	        return 1;
    		}
    		else if (hasChildLink(target, source)) {
    			if (log.isDebugEnabled())
    				log.debug("(return -1) singular link from : "
    	                + target.toString() + " to: " + source.toString());
    	        return -1;
    		}
    		else {
    			// For say a root object which is part of
    			// a tree, where the property e.g. 'parent'
    			// is null the above link check won't apply, 
    			// Therefore rely on graph depth 
    			if (sourceDepth < targetDepth) {
        			if (log.isDebugEnabled())
        				log.debug("depth: "
        	                + sourceDepth + " / " + targetDepth);
        	        return -1;
    			}
    			else if (targetDepth < sourceDepth) {
        			if (log.isDebugEnabled())
        				log.debug("depth: "
        	                + targetDepth + " / " + sourceDepth);
        	        return -1;
    			}
    			else
    			    return 0; 
    		}
	    }
	     
	}

}
