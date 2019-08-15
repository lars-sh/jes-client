package de.larssh.jes;

import static de.larssh.utils.test.Assertions.assertEqualsAndHashCode;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import de.larssh.utils.test.AssertEqualsAndHashCodeArguments;
import lombok.NoArgsConstructor;

/**
 * {@link Job}
 */
@NoArgsConstructor
public class JobTest {
	private static final Job A = new Job("a", JesClient.FILTER_WILDCARD, JobStatus.ACTIVE, "m");

	private static final Job B = new Job("b ",
			"",
			JobStatus.ALL,
			"n",
			Optional.empty(),
			OptionalInt.empty(),
			Optional.of("u "),
			JobFlag.DUP);

	private static final Job C = new Job("c", "g* ", JobStatus.INPUT, "o");

	private static final Job D = new Job("d ",
			"h",
			JobStatus.OUTPUT,
			"p ",
			Optional.of("r "),
			OptionalInt.of(20),
			Optional.empty(),
			JobFlag.HELD,
			JobFlag.JCL_ERROR);

	/**
	 * {@link Job#createOutput(int, String, int, Optional, Optional, Optional)}
	 */
	@Test
	public void testCreateOutput() {
		final Job job = new Job("a", "b", JobStatus.OUTPUT, "d");
		final JobOutput jobOutputA
				= new JobOutput(job, 1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty());
		final JobOutput jobOutputB
				= new JobOutput(job, 3, "d", 7, Optional.of("h"), Optional.of("j"), Optional.of("l"));

		assertEquals(emptyList(), job.getOutputs());
		assertEquals(jobOutputA, job.createOutput(1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty()));
		assertEquals(singletonList(jobOutputA), job.getOutputs());
		assertEquals(jobOutputB, job.createOutput(3, "d", 7, Optional.of("h"), Optional.of("j"), Optional.of("k")));
		assertEquals(Arrays.asList(jobOutputA, jobOutputB), job.getOutputs());

		final Job jobAll = new Job("a", "b", JobStatus.ALL, "d");
		assertDoesNotThrow(() -> jobAll.createOutput(1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty()));

		final Job jobActive = new Job("a", "b", JobStatus.ACTIVE, "d");
		assertThrows(JobFieldInconsistentException.class,
				() -> jobActive.createOutput(1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty()));

		final Job jobInput = new Job("a", "b", JobStatus.INPUT, "d");
		assertThrows(JobFieldInconsistentException.class,
				() -> jobInput.createOutput(1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty()));
	}

	/**
	 * {@link Job#equals(Object)} and {@link Job#hashCode()}
	 */
	@Test
	public void testEqualsAndHashCode() {
		assertEqualsAndHashCode(Job.class,
				new AssertEqualsAndHashCodeArguments().add("A", "B", false)
						.add("C", "D", true)
						.add(JobStatus.ACTIVE, JobStatus.ALL, true)
						.add("G", "H", true));

		assertEqualsAndHashCode(Job.class,
				new AssertEqualsAndHashCodeArguments().add("A", "B", false)
						.add("C", "D", true)
						.add(JobStatus.ALL, JobStatus.OUTPUT, true)
						.add("G", "H", true)
						.add(Optional.empty(), Optional.of("J"), true)
						.add(OptionalInt.empty(), OptionalInt.of(12), true)
						.add(Optional.empty(), Optional.empty(), true)
						.add(new JobFlag[0], new JobFlag[] { JobFlag.DUP }, true));

		assertEqualsAndHashCode(Job.class,
				new AssertEqualsAndHashCodeArguments().add("A", "B", false)
						.add("C", "D", true)
						.add(JobStatus.ALL, JobStatus.OUTPUT, true)
						.add("G", "H", true)
						.add(Optional.empty(), Optional.empty(), true)
						.add(OptionalInt.empty(), OptionalInt.of(12), true)
						.add(Optional.empty(), Optional.of("N"), true)
						.add(new JobFlag[0], new JobFlag[] { JobFlag.DUP }, true));
	}

	/**
	 * {@link Job#getAbendCode()}
	 */
	@Test
	public void testGetAbendCode() {
		assertEquals(Optional.empty(), A.getAbendCode());
		assertEquals(Optional.of("U"), B.getAbendCode());
		assertEquals(Optional.empty(), C.getAbendCode());
		assertEquals(Optional.empty(), D.getAbendCode());
		assertThrows(JobFieldInconsistentException.class,
				() -> new Job("a",
						"b",
						JobStatus.OUTPUT,
						"c",
						Optional.empty(),
						OptionalInt.empty(),
						Optional.of(" ")));
		assertThrows(JobFieldInconsistentException.class,
				() -> new Job("a", "b", JobStatus.INPUT, "c", Optional.empty(), OptionalInt.empty(), Optional.of("f")));
	}

	/**
	 * {@link Job#getId()}
	 */
	@Test
	public void testGetId() {
		assertEquals("A", A.getId());
		assertEquals("B", B.getId());
		assertEquals("C", C.getId());
		assertEquals("D", D.getId());
		assertThrows(JobFieldInconsistentException.class, () -> new Job(" ", "b", JobStatus.INPUT, "c"));
	}

	/**
	 * {@link Job#getFlags()}
	 */
	@Test
	public void testGetFlags() {
		assertEquals(emptySet(), A.getFlags());
		assertEquals(singleton(JobFlag.DUP), B.getFlags());
		assertEquals(emptySet(), C.getFlags());
		assertEquals(new HashSet<>(asList(JobFlag.HELD, JobFlag.JCL_ERROR)), D.getFlags());
	}

	/**
	 * {@link Job#getJesClass()}
	 */
	@Test
	public void testGetJesClass() {
		assertEquals(Optional.empty(), A.getJesClass());
		assertEquals(Optional.empty(), B.getJesClass());
		assertEquals(Optional.empty(), C.getJesClass());
		assertEquals(Optional.of("R"), D.getJesClass());
		assertThrows(JobFieldInconsistentException.class,
				() -> new Job("a", "b", JobStatus.INPUT, "c", Optional.of(" "), OptionalInt.empty(), Optional.empty()));
	}

	/**
	 * {@link Job#getName()}
	 */
	@Test
	public void testGetName() {
		assertEquals(JesClient.FILTER_WILDCARD, A.getName());
		assertEquals("", B.getName());
		assertEquals("G*", C.getName());
		assertEquals("H", D.getName());
	}

	/**
	 * {@link Job#getOutput(String)}
	 */
	@Test
	public void testGetOutput() {
		final Job job = new Job("a", "b", JobStatus.OUTPUT, "d");
		job.createOutput(1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty());
		job.createOutput(2, "d", 0, Optional.empty(), Optional.empty(), Optional.empty());
		job.createOutput(3, "e", 0, Optional.empty(), Optional.empty(), Optional.empty());

		final Optional<JobOutput> jobOutput
				= Optional.of(new JobOutput(job, 2, "d", 0, Optional.empty(), Optional.empty(), Optional.empty()));

		assertEquals(jobOutput, job.getOutput("D"));
		assertEquals(jobOutput, job.getOutput("d"));
		assertEquals(Optional.empty(), job.getOutput("z"));
	}

	/**
	 * {@link Job#getOutputs()}
	 */
	@Test
	public void testGetOutputs() {
		assertEquals(emptyList(), A.getOutputs());
		assertEquals(emptyList(), B.getOutputs());
		assertEquals(emptyList(), C.getOutputs());
		assertEquals(emptyList(), D.getOutputs());
	}

	/**
	 * {@link Job#getOwner()}
	 */
	@Test
	public void testGetOwner() {
		assertEquals("M", A.getOwner());
		assertEquals("N", B.getOwner());
		assertEquals("O", C.getOwner());
		assertEquals("P", D.getOwner());
	}

	/**
	 * {@link Job#getResultCode()}
	 */
	@Test
	public void testGetResultCode() {
		assertEquals(OptionalInt.empty(), A.getResultCode());
		assertEquals(OptionalInt.empty(), B.getResultCode());
		assertEquals(OptionalInt.empty(), C.getResultCode());
		assertEquals(OptionalInt.of(20), D.getResultCode());

		assertEquals(OptionalInt.of(6),
				new Job("a", "b", JobStatus.OUTPUT, "d", Optional.empty(), OptionalInt.of(6), Optional.empty())
						.getResultCode());
		assertThrows(JobFieldInconsistentException.class,
				() -> new Job("a", "b", JobStatus.OUTPUT, "d", Optional.empty(), OptionalInt.of(-6), Optional.empty()));
		assertThrows(JobFieldInconsistentException.class,
				() -> new Job("a", "b", JobStatus.OUTPUT, "d", Optional.empty(), OptionalInt.of(6), Optional.of("g")));
		assertThrows(JobFieldInconsistentException.class,
				() -> new Job("a", "b", JobStatus.ACTIVE, "d", Optional.empty(), OptionalInt.of(6), Optional.empty()));
		assertThrows(JobFieldInconsistentException.class,
				() -> new Job("a", "b", JobStatus.INPUT, "d", Optional.empty(), OptionalInt.of(6), Optional.empty()));
	}

	/**
	 * {@link Job#getStatus()}
	 */
	@Test
	public void testGetStatus() {
		assertEquals(JobStatus.ACTIVE, A.getStatus());
		assertEquals(JobStatus.ALL, B.getStatus());
		assertEquals(JobStatus.INPUT, C.getStatus());
		assertEquals(JobStatus.OUTPUT, D.getStatus());
	}
}
