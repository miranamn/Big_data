file:///C:/Users/gleba/flink-training-exercises/src/test/java/com/ververica/flinktraining/exercises/datastream_java/windows/HourlyTipsScalaTest.java
### java.util.NoSuchElementException: next on empty iterator

occurred in the presentation compiler.

action parameters:
offset: 268
uri: file:///C:/Users/gleba/flink-training-exercises/src/test/java/com/ververica/flinktraining/exercises/datastream_java/windows/HourlyTipsScalaTest.java
text:
```scala
/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.a@@pache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_scala.windows.HourlyTipsExercise;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;


public class HourlyTipsScalaTest extends HourlyTipsTest {

	static Testable scalaExercise = () -> HourlyTipsExercise.main(new String[]{});

	protected List<Tuple3<Long, Long, Float>> results(TestFareSource source) throws Exception {
		Testable scalaSolution = () -> com.ververica.flinktraining.solutions.datastream_scala.windows.HourlyTipsSolution.main(new String[]{});
		List<?> tuples = runApp(source, new TestSink<>(), scalaExercise, scalaSolution);
		return javaTuples((ArrayList<scala.Tuple3<Long, Long, Float>>) tuples);
	}

	private ArrayList<Tuple3<Long, Long, Float>> javaTuples(ArrayList<scala.Tuple3<Long, Long, Float>> a) {
		ArrayList<Tuple3<Long, Long, Float>> javaCopy = new ArrayList<>(a.size());
		a.iterator().forEachRemaining(t -> javaCopy.add(new Tuple3(t._1(), t._2(), t._3())));
		return javaCopy;
	}

}
```



#### Error stacktrace:

```
scala.collection.Iterator$$anon$19.next(Iterator.scala:973)
	scala.collection.Iterator$$anon$19.next(Iterator.scala:971)
	scala.collection.mutable.MutationTracker$CheckedIterator.next(MutationTracker.scala:76)
	scala.collection.IterableOps.head(Iterable.scala:222)
	scala.collection.IterableOps.head$(Iterable.scala:222)
	scala.collection.AbstractIterable.head(Iterable.scala:933)
	dotty.tools.dotc.interactive.InteractiveDriver.run(InteractiveDriver.scala:168)
	scala.meta.internal.pc.MetalsDriver.run(MetalsDriver.scala:45)
	scala.meta.internal.pc.HoverProvider$.hover(HoverProvider.scala:34)
	scala.meta.internal.pc.ScalaPresentationCompiler.hover$$anonfun$1(ScalaPresentationCompiler.scala:329)
```
#### Short summary: 

java.util.NoSuchElementException: next on empty iterator