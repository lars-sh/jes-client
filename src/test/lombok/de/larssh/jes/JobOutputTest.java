package de.larssh.jes;

import static de.larssh.utils.test.Assertions.assertEqualsAndHashCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import de.larssh.utils.test.AssertEqualsAndHashCodeArguments;
import lombok.NoArgsConstructor;

/**
 * {@link JobOutput}
 */
@NoArgsConstructor
public class JobOutputTest {
	private static final Job JOB = new Job("a", "b", JobStatus.OUTPUT, "d");

	private static final JobOutput A
			= new JobOutput(JOB, 1, "c", 0, Optional.empty(), Optional.empty(), Optional.empty());

	private static final JobOutput B
			= new JobOutput(JOB, 3, "d ", 7, Optional.of("h "), Optional.of("j "), Optional.of("l "));

	/**
	 * {@link JobOutput#equals(Object)} and {@link JobOutput#hashCode()}
	 */
	@Test
	public void testEqualsAndHashCode() {
		assertEqualsAndHashCode(JobOutput.class,
				new AssertEqualsAndHashCodeArguments().add(JOB, new Job("e", "f", JobStatus.ALL, "h"), false)
						.add(1, 3, false)
						.add("c", "d", true)
						.add(0, 7, true)
						.add(Optional.empty(), Optional.of("h"), true)
						.add(Optional.empty(), Optional.of("j"), true)
						.add(Optional.empty(), Optional.of("l"), true));
	}

	/**
	 * {@link JobOutput#getIndex()}
	 */
	@Test
	public void testGetIndex() {
		assertEquals(1, A.getIndex());
		assertEquals(3, B.getIndex());
		assertThrows(JobFieldInconsistentException.class,
				() -> new JobOutput(JOB, -1, "a", 0, Optional.empty(), Optional.empty(), Optional.empty()));
		assertThrows(JobFieldInconsistentException.class,
				() -> new JobOutput(JOB, 0, "a", 0, Optional.empty(), Optional.empty(), Optional.empty()));
	}

	/**
	 * {@link JobOutput#getJob()}
	 */
	@Test
	public void testGetJob() {
		assertEquals(JOB, A.getJob());
		assertEquals(JOB, B.getJob());

		final Job job = new Job("e", "f", JobStatus.ALL, "h");
		assertEquals(job, new JobOutput(job, 1, "a", 0, Optional.empty(), Optional.empty(), Optional.empty()).getJob());
	}

	/**
	 * {@link JobOutput#getLength()}
	 */
	@Test
	public void testGetLength() {
		assertEquals(0, A.getLength());
		assertEquals(7, B.getLength());
		assertThrows(JobFieldInconsistentException.class,
				() -> new JobOutput(JOB, 1, "a", -1, Optional.empty(), Optional.empty(), Optional.empty()));
	}

	/**
	 * {@link JobOutput#getName()}
	 */
	@Test
	public void testGetName() {
		assertEquals("C", A.getName());
		assertEquals("D", B.getName());
		assertThrows(JobFieldInconsistentException.class,
				() -> new JobOutput(JOB, 1, "", 0, Optional.empty(), Optional.empty(), Optional.empty()));
		assertThrows(JobFieldInconsistentException.class,
				() -> new JobOutput(JOB, 1, " ", 0, Optional.empty(), Optional.empty(), Optional.empty()));
	}

	/**
	 * {@link JobOutput#getOutputClass()}
	 */
	@Test
	public void testGetOutputClass() {
		assertEquals(Optional.empty(), A.getOutputClass());
		assertEquals(Optional.of("L"), B.getOutputClass());
		assertThrows(JobFieldInconsistentException.class,
				() -> new JobOutput(JOB, 1, "a", 0, Optional.empty(), Optional.of(" "), Optional.empty()));
	}

	/**
	 * {@link JobOutput#getProcedureStep()}
	 */
	@Test
	public void testGetProcedureStep() {
		assertEquals(Optional.empty(), A.getProcedureStep());
		assertEquals(Optional.of("J"), B.getProcedureStep());
		assertThrows(JobFieldInconsistentException.class,
				() -> new JobOutput(JOB, 1, "a", 0, Optional.empty(), Optional.empty(), Optional.of(" ")));
	}

	/**
	 * {@link JobOutput#getStep()}
	 */
	@Test
	public void testGetStep() {
		assertEquals(Optional.empty(), A.getStep());
		assertEquals(Optional.of("H"), B.getStep());
		assertThrows(JobFieldInconsistentException.class,
				() -> new JobOutput(JOB, 1, "a", 0, Optional.of(" "), Optional.empty(), Optional.empty()));
	}
}
