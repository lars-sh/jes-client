package de.larssh.jes;

import static java.util.Collections.unmodifiableList;

import java.util.List;

import org.apache.commons.net.ftp.FTPClient;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Thrown to indicate that a JES limit has been reached and contains the job
 * entries up to the specified JES limit.
 */
@Getter
@ToString(callSuper = false)
@EqualsAndHashCode(callSuper = true)
public class JesLimitReachedException extends JesException {
	/**
	 * Limit of spool entries
	 *
	 * @return limit of spool entries
	 */
	int limit;

	/**
	 * List of jobs up to the JES limit
	 */
	transient List<Job> jobs;

	/**
	 * Constructs a new {@link JesLimitReachedException} with a default detail
	 * message and a given list of jobs up to the limit of spool entries.
	 *
	 * @param limit     limit of spool entries
	 * @param jobs      list of jobs up to the limit
	 * @param ftpClient FTP client with reply string
	 */
	public JesLimitReachedException(final int limit, final List<Job> jobs, final FTPClient ftpClient) {
		super(ftpClient, "Listing limit of %d reached.", limit);

		this.limit = limit;
		this.jobs = jobs;
	}

	/**
	 * List of jobs up to the limit
	 *
	 * @return list of jobs
	 */
	public List<Job> getJobs() {
		return unmodifiableList(jobs);
	}

	/** {@inheritDoc} */
	@Override
	@ToString.Include(name = "message", rank = 1)
	@SuppressWarnings("PMD.UselessOverridingMethod")
	public String getMessage() {
		return super.getMessage();
	}
}
