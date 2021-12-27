package de.larssh.jes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Optional;

import org.junit.jupiter.api.Test;

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
	 * {@link JobOutput#getIndex()}
	 */
	@Test
	public void testGetIndex() {
		assertThat(1).isEqualTo(A.getIndex());
		assertThat(3).isEqualTo(B.getIndex());
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new JobOutput(JOB, -1, "a", 0, Optional.empty(), Optional.empty(), Optional.empty()));
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new JobOutput(JOB, 0, "a", 0, Optional.empty(), Optional.empty(), Optional.empty()));
	}

	/**
	 * {@link JobOutput#getJob()}
	 */
	@Test
	public void testGetJob() {
		assertThat(JOB).isEqualTo(A.getJob());
		assertThat(JOB).isEqualTo(B.getJob());

		final Job job = new Job("e", "f", JobStatus.ALL, "h");
		assertThat(job).isEqualTo(
				new JobOutput(job, 1, "a", 0, Optional.empty(), Optional.empty(), Optional.empty()).getJob());
	}

	/**
	 * {@link JobOutput#getLength()}
	 */
	@Test
	public void testGetLength() {
		assertThat(0).isEqualTo(A.getLength());
		assertThat(7).isEqualTo(B.getLength());
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new JobOutput(JOB, 1, "a", -1, Optional.empty(), Optional.empty(), Optional.empty()));
	}

	/**
	 * {@link JobOutput#getName()}
	 */
	@Test
	public void testGetName() {
		assertThat("C").isEqualTo(A.getName());
		assertThat("D").isEqualTo(B.getName());
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new JobOutput(JOB, 1, "", 0, Optional.empty(), Optional.empty(), Optional.empty()));
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new JobOutput(JOB, 1, " ", 0, Optional.empty(), Optional.empty(), Optional.empty()));
	}

	/**
	 * {@link JobOutput#getOutputClass()}
	 */
	@Test
	public void testGetOutputClass() {
		assertThat(Optional.empty()).isEqualTo(A.getOutputClass());
		assertThat(Optional.of("L")).isEqualTo(B.getOutputClass());
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new JobOutput(JOB, 1, "a", 0, Optional.empty(), Optional.of(" "), Optional.empty()));
	}

	/**
	 * {@link JobOutput#getProcedureStep()}
	 */
	@Test
	public void testGetProcedureStep() {
		assertThat(Optional.empty()).isEqualTo(A.getProcedureStep());
		assertThat(Optional.of("J")).isEqualTo(B.getProcedureStep());
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new JobOutput(JOB, 1, "a", 0, Optional.empty(), Optional.empty(), Optional.of(" ")));
	}

	/**
	 * {@link JobOutput#getStep()}
	 */
	@Test
	public void testGetStep() {
		assertThat(Optional.empty()).isEqualTo(A.getStep());
		assertThat(Optional.of("H")).isEqualTo(B.getStep());
		assertThatExceptionOfType(JobFieldInconsistentException.class)
				.isThrownBy(() -> new JobOutput(JOB, 1, "a", 0, Optional.of(" "), Optional.empty(), Optional.empty()));
	}
}
