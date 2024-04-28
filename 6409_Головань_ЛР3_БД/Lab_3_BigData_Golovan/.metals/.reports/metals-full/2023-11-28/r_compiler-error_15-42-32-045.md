file:///C:/Users/gleba/flink-training-exercises/src/test/java/com/ververica/flinktraining/exercises/datastream_java/state/RidesAndFaresTest.java
### java.util.NoSuchElementException: next on empty iterator

occurred in the presentation compiler.

action parameters:
offset: 851
uri: file:///C:/Users/gleba/flink-training-exercises/src/test/java/com/ververica/flinktraining/exercises/datastream_java/state/RidesAndFaresTest.java
text:
```scala
/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java@@.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.ververica.flinktraining.solutions.datastream_java.state.RidesAndFaresSolution;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.joda.time.DateTime;
import org.junit.Test;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

public class RidesAndFaresTest extends TaxiRideTestBase<Tuple2<TaxiRide, TaxiFare>> {

	static Testable javaExercise = () -> RidesAndFaresExercise.main(new String[]{});


	final TaxiRide ride1 = testRide(1);
	final TaxiRide ride2 = testRide(2);
	final TaxiFare fare1 = testFare(1);
	final TaxiFare fare2 = testFare(2);

	@Test
	public void testInOrder() throws Exception {
		TestRideSource rides = new TestRideSource(ride1, ride2);
		TestFareSource fares = new TestFareSource(fare1, fare2);

		ArrayList<Tuple2<TaxiRide, TaxiFare>> expected = Lists.newArrayList(
				new Tuple2<>(ride1, fare1),
				new Tuple2<>(ride2, fare2));

		assertThat("Join results don't match", results(rides, fares), containsInAnyOrder(expected.toArray()));
	}

	@Test
	public void testOutOfOrder() throws Exception {
		TestRideSource rides = new TestRideSource(ride1, ride2);
		TestFareSource fares = new TestFareSource(fare2, fare1);

		ArrayList<Tuple2<TaxiRide, TaxiFare>> expected = Lists.newArrayList(
				new Tuple2<>(ride1, fare1),
				new Tuple2<>(ride2, fare2));

		assertThat("Join results don't match", results(rides, fares), containsInAnyOrder(expected.toArray()));
	}

	private TaxiRide testRide(long rideId) {
		return new TaxiRide(rideId, true, new DateTime(0), new DateTime(0),
				0F, 0F, 0F, 0F, (short)1, 0, rideId);
	}

	private TaxiFare testFare(long rideId) {
		return new TaxiFare(rideId, 0, rideId, new DateTime(0), "", 0F, 0F, 0F);
	}

	protected List<?> results(TestRideSource rides, TestFareSource fares) throws Exception {
		Testable javaSolution = () -> RidesAndFaresSolution.main(new String[]{});
		return runApp(rides, fares, new TestSink<>(), javaExercise, javaSolution);
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