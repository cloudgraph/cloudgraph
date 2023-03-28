/**
 * Copyright 2017 TerraMeta Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudgraph.state;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import jakarta.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.test.socialgraph.actor.Actor;
import org.cloudgraph.test.socialgraph.actor.Friendship;
import org.cloudgraph.test.socialgraph.story.Blog;
import org.cloudgraph.test.socialgraph.story.Story;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.helper.PlasmaTypeHelper;
import org.xml.sax.SAXException;

/**
 * State marshalling related tests.
 * 
 * @author Scott Cinnamond
 * @since 0.5.2
 */
public class StateMarshalingTestCase extends StateTestCase {
  private static Log log = LogFactory.getLog(StateMarshalingTestCase.class);

  private ValidatingDataBinding binding;

  public void setUp() throws Exception {
  }

  public void testProtoMarshal() throws JAXBException, SAXException, FileNotFoundException {
    SequenceGenerator state = new ProtoSequenceGenerator(/*
                                                          * java.util.UUID.
                                                          * randomUUID()
                                                          */);
    PlasmaType typeActor = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(Actor.class);
    assertFalse(state.hasLastSequence(typeActor));
    state.nextSequence(typeActor);
    state.nextSequence(typeActor);
    state.nextSequence(typeActor);
    assertTrue(state.hasLastSequence(typeActor));
    assertTrue(state.lastSequence(typeActor) == 3);

    PlasmaType typeBlog = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(Blog.class);
    assertFalse(state.hasLastSequence(typeBlog));
    state.nextSequence(typeBlog);
    state.nextSequence(typeBlog);
    assertTrue(state.hasLastSequence(typeBlog));
    assertTrue(state.lastSequence(typeBlog) == 2);

    PlasmaType typeStory = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(Story.class);
    assertFalse(state.hasLastSequence(typeStory));
    state.nextSequence(typeStory);
    assertTrue(state.hasLastSequence(typeStory));
    state.nextSequence(typeStory);
    state.nextSequence(typeStory);
    state.nextSequence(typeStory);
    state.nextSequence(typeStory);
    assertTrue(state.hasLastSequence(typeStory));
    assertTrue(state.lastSequence(typeStory) == 5);

    byte[] content = state.marshal();

    // unmarshal current state and check it
    state = new ProtoSequenceGenerator(content);
    assertTrue(state.hasLastSequence(typeActor));
    assertTrue(state.lastSequence(typeActor) == 3);
    assertTrue(state.hasLastSequence(typeBlog));
    assertTrue(state.lastSequence(typeBlog) == 2);
    assertTrue(state.hasLastSequence(typeStory));
    assertTrue(state.lastSequence(typeStory) == 5);

    PlasmaType typeFriendship = (PlasmaType) PlasmaTypeHelper.INSTANCE.getType(Friendship.class);
    assertFalse(state.hasLastSequence(typeFriendship));
    state.nextSequence(typeFriendship);
    assertTrue(state.hasLastSequence(typeFriendship));
    state.nextSequence(typeFriendship);
    state.nextSequence(typeFriendship);
    assertTrue(state.hasLastSequence(typeFriendship));
    assertTrue(state.lastSequence(typeFriendship) == 3);

    content = state.marshal();

  }

}