package org.cloudgraph.store.mapping.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.cloudgraph.store.mapping.KeyFieldCodecType;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RowKeyField {
  public KeyFieldCodecType codecType() default KeyFieldCodecType.PAD;
}
