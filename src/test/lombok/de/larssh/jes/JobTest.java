package de.larssh.jes;

import static de.larssh.utils.test.Assertions.assertEqualsAndHashCode;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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

		assertThat(emptyList()).isEqualTo(job.getOutputs());
		assertThat(jobOutputA)
				.isEqualTo(job.createOutput(1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty()));
		assertThat(singletonList(jobOutputA)).isEqualTo(job.getOutputs());
		assertThat(jobOutputB)
				.isEqualTo(job.createOutput(3, "d", 7, Optional.of("h"), Optional.of("j"), Optional.of("k")));
		assertThat(Arrays.asList(jobOutputA, jobOutputB)).isEqualTo(job.getOutputs());

		final Job jobAll = new Job("a", "b", JobStatus.ALL, "d");
		jobAll.createOutput(1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty());

		final Job jobActive = new Job("a", "b", JobStatus.ACTIVE, "d");
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(
				() -> jobActive.createOutput(1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty()));

		final Job jobInput = new Job("a", "b", JobStatus.INPUT, "d");
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(
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
		assertThat(Optional.empty()).isEqualTo(A.getAbendCode());
		assertThat(Optional.of("U")).isEqualTo(B.getAbendCode());
		assertThat(Optional.empty()).isEqualTo(C.getAbendCode());
		assertThat(Optional.empty()).isEqualTo(D.getAbendCode());
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(() -> new Job("a",
				"b",
				JobStatus.OUTPUT,
				"c",
				Optional.empty(),
				OptionalInt.empty(),
				Optional.of(" ")));
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(
				() -> new Job("a", "b", JobStatus.INPUT, "c", Optional.empty(), OptionalInt.empty(), Optional.of("f")));
	}

	/**
	 * {@link Job#getId()}
	 */
	@Test
	public void testGetId() {
		assertThat("A").isEqualTo(A.getId());
		assertThat("B").isEqualTo(B.getId());
		assertThat("C").isEqualTo(C.getId());
		assertThat("D").isEqualTo(D.getId());
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new Job(" ", "b", JobStatus.INPUT, "c"));
	}

	/**
	 * {@link Job#getFlags()}
	 */
	@Test
	public void testGetFlags() {
		assertThat(emptySet()).isEqualTo(A.getFlags());
		assertThat(singleton(JobFlag.DUP)).isEqualTo(B.getFlags());
		assertThat(emptySet()).isEqualTo(C.getFlags());
		assertThat(new HashSet<>(asList(JobFlag.HELD, JobFlag.JCL_ERROR))).isEqualTo(D.getFlags());
	}

	/**
	 * {@link Job#getJesClass()}
	 */
	@Test
	public void testGetJesClass() {
		assertThat(Optional.empty()).isEqualTo(A.getJesClass());
		assertThat(Optional.empty()).isEqualTo(B.getJesClass());
		assertThat(Optional.empty()).isEqualTo(C.getJesClass());
		assertThat(Optional.of("R")).isEqualTo(D.getJesClass());
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(
				() -> new Job("a", "b", JobStatus.INPUT, "c", Optional.of(" "), OptionalInt.empty(), Optional.empty()));
	}

	/**
	 * {@link Job#getName()}
	 */
	@Test
	public void testGetName() {
		assertThat(JesClient.FILTER_WILDCARD).isEqualTo(A.getName());
		assertThat("").isEqualTo(B.getName());
		assertThat("G*").isEqualTo(C.getName());
		assertThat("H").isEqualTo(D.getName());
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

		assertThat(jobOutput).isEqualTo(job.getOutput("D"));
		assertThat(jobOutput).isEqualTo(job.getOutput("d"));
		assertThat(Optional.empty()).isEqualTo(job.getOutput("z"));
	}

	/**
	 * {@link Job#getOutputs()}
	 */
	@Test
	public void testGetOutputs() {
		assertThat(emptyList()).isEqualTo(A.getOutputs());
		assertThat(emptyList()).isEqualTo(B.getOutputs());
		assertThat(emptyList()).isEqualTo(C.getOutputs());
		assertThat(emptyList()).isEqualTo(D.getOutputs());
	}

	/**
	 * {@link Job#getOwner()}
	 */
	@Test
	public void testGetOwner() {
		assertThat("M").isEqualTo(A.getOwner());
		assertThat("N").isEqualTo(B.getOwner());
		assertThat("O").isEqualTo(C.getOwner());
		assertThat("P").isEqualTo(D.getOwner());
	}

	/**
	 * {@link Job#getResultCode()}
	 */
	@Test
	public void testGetResultCode() {
		assertThat(OptionalInt.empty()).isEqualTo(A.getResultCode());
		assertThat(OptionalInt.empty()).isEqualTo(B.getResultCode());
		assertThat(OptionalInt.empty()).isEqualTo(C.getResultCode());
		assertThat(OptionalInt.of(20)).isEqualTo(D.getResultCode());

		assertThat(OptionalInt.of(6)).isEqualTo(
				new Job("a", "b", JobStatus.OUTPUT, "d", Optional.empty(), OptionalInt.of(6), Optional.empty())
						.getResultCode());
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(
				() -> new Job("a", "b", JobStatus.OUTPUT, "d", Optional.empty(), OptionalInt.of(-6), Optional.empty()));
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(
				() -> new Job("a", "b", JobStatus.OUTPUT, "d", Optional.empty(), OptionalInt.of(6), Optional.of("g")));
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(
				() -> new Job("a", "b", JobStatus.ACTIVE, "d", Optional.empty(), OptionalInt.of(6), Optional.empty()));
		assertThatExceptionOfType(JobFieldInconsistentException.class).isThrownBy(
				() -> new Job("a", "b", JobStatus.INPUT, "d", Optional.empty(), OptionalInt.of(6), Optional.empty()));
	}

	/**
	 * {@link Job#getStatus()}
	 */
	@Test
	public void testGetStatus() {
		assertThat(JobStatus.ACTIVE).isEqualTo(A.getStatus());
		assertThat(JobStatus.ALL).isEqualTo(B.getStatus());
		assertThat(JobStatus.INPUT).isEqualTo(C.getStatus());
		assertThat(JobStatus.OUTPUT).isEqualTo(D.getStatus());
	}
}
