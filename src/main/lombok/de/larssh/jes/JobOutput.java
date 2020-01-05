package de.larssh.jes;

import java.util.Optional;

import de.larssh.utils.annotations.SuppressJacocoGenerated;
import de.larssh.utils.text.Strings;
import edu.umd.cs.findbugs.annotations.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Value object containing a job outputs details.
 */
@Getter
@ToString
@SuppressWarnings("PMD.DataClass")
@EqualsAndHashCode(onlyExplicitlyIncluded = true, onParam_ = { @Nullable })
public class JobOutput {
	/**
	 * Corresponding {@link Job}
	 *
	 * @return job
	 */
	@ToString.Exclude
	@EqualsAndHashCode.Include
	Job job;

	/**
	 * The job outputs index inside the jobs list of outputs (starting at 1)
	 *
	 * @return job output index
	 */
	@EqualsAndHashCode.Include
	int index;

	/**
	 * The job outputs data division name
	 *
	 * @return job output name
	 */
	String name;

	/**
	 * The job outputs content length
	 *
	 * @return job output length
	 */
	int length;

	/**
	 * The job outputs step name
	 *
	 * @return step
	 */
	Optional<String> step;

	/**
	 * The job outputs procedure step name
	 *
	 * @return procedure step
	 */
	Optional<String> procedureStep;

	/**
	 * The job outputs class
	 *
	 * @return output class
	 */
	Optional<String> outputClass;

	/**
	 * This constructor creates a {@link JobOutput}. String parameters are trimmed
	 * and converted to upper case.
	 *
	 * @param job           the corresponding job
	 * @param index         the job outputs index inside the jobs list of outputs
	 *                      (starting at 1)
	 * @param name          the job outputs data division name
	 * @param length        the job outputs content length
	 * @param step          the job outputs step name
	 * @param procedureStep the job outputs procedure step name
	 * @param outputClass   the output class
	 * @throws JobFieldInconsistentException on inconsistent field value
	 */
	protected JobOutput(final Job job,
			final int index,
			final String name,
			final int length,
			final Optional<String> step,
			final Optional<String> procedureStep,
			final Optional<String> outputClass) {
		this.job = job;
		this.index = index;
		this.name = Strings.toNeutralUpperCase(name.trim());
		this.length = length;
		this.step = step.map(String::trim).map(Strings::toNeutralUpperCase);
		this.procedureStep = procedureStep.map(String::trim).map(Strings::toNeutralUpperCase);
		this.outputClass = outputClass.map(String::trim).map(Strings::toNeutralUpperCase);

		validate();
	}

	/**
	 * Corresponding {@link Job}s ID for {@link lombok.ToString.Include}
	 *
	 * @return the jobs ID
	 */
	@ToString.Include(name = "job.id", rank = 1)
	@SuppressJacocoGenerated(justification = "private method, used for toString only")
	private String getJobId() {
		return getJob().getId();
	}

	/**
	 * Validates fields to be set in a consistent way.
	 *
	 * @throws JobFieldInconsistentException on inconsistent field value
	 */
	@SuppressWarnings("PMD.CyclomaticComplexity")
	private void validate() {
		if (index < 1) {
			throw new JobFieldInconsistentException("Index must not be less than one.");
		}
		if (name.isEmpty()) {
			throw new JobFieldInconsistentException("Name must not be empty.");
		}
		if (length < 0) {
			throw new JobFieldInconsistentException("Length must not be less than zero.");
		}
		if (step.filter(String::isEmpty).isPresent()) {
			throw new JobFieldInconsistentException("Step must not be empty if present.");
		}
		if (procedureStep.filter(String::isEmpty).isPresent()) {
			throw new JobFieldInconsistentException("Procedure Step must not be empty if present.");
		}
		if (outputClass.filter(String::isEmpty).isPresent()) {
			throw new JobFieldInconsistentException("Output class must not be empty if present.");
		}
	}
}
