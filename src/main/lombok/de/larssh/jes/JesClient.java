package de.larssh.jes;

import static de.larssh.utils.Collectors.toLinkedHashMap;
import static de.larssh.utils.Finals.constant;
import static java.util.stream.Collectors.toList;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import de.larssh.jes.parser.JesFtpFile;
import de.larssh.jes.parser.JesFtpFileEntryParserFactory;
import de.larssh.utils.Optionals;
import de.larssh.utils.function.ThrowingFunction;
import de.larssh.utils.function.ThrowingRunnable;
import de.larssh.utils.text.Patterns;
import de.larssh.utils.text.Strings;
import lombok.Getter;
import lombok.experimental.NonFinal;

/**
 * This class allows to handle IBM z/OS JES spools using Java technologies. The
 * used interface is the IBM z/OS FTP server, that should be available by
 * default.
 *
 * <p>
 * JES spool entries can be filtered and listed using
 * {@link #list(String, JobStatus, String)} and
 * {@link #listFilled(String, JobStatus, String)} methods, while the later one
 * gathers more information, but takes some more time.
 *
 * <p>
 * {@link #submit(String)} submits JCLs based on the FTP users permissions.
 * {@link #waitFor(Job)} can be used to wait until a job terminates. Job outputs
 * can be retrieved using {@link #retrieve(JobOutput)} and removed using
 * {@link #delete(Job)}.
 *
 * <p>
 * <b>Usage example:</b> The following shows the JesClient used inside a
 * try-with-resource statement. The constructor descriptions describe further
 * details.
 *
 * <pre>
 * // Connect and login via simplified constructor
 * try (JesClient jesClient = new JesClient(hostname, port, username, password)) {
 *
 *     // Submit JCL
 *     Job submittedJob = jesClient.submit(jclContent);
 *
 *     // Wait for submitted job to be finished
 *     Optional&lt;Job&gt; finishedJob = jesClient.waitFor(submittedJob);
 *
 *     // Handle the case, a finished job cannot be found inside JES spool any longer
 *     if (!finishedJob.isPresent()) {
 *         ...
 *     } else {
 *
 *         // Gather finished jobs outputs
 *         List&lt;JobOutput&gt; jobOutput = jesClient.get(finishedJob.get());
 *
 *         // Delete job from JES spool
 *         jesClient.delete(finishedJob.get());
 *     }
 *
 * // Logout and disconnect using try-with-resource (close method)
 * }
 * </pre>
 *
 * <p>
 * In case filtering jobs does not work as expected, check the JES Interface
 * Level of your server using {@link #getServerProperties()}. This class
 * requires {@code JESINTERFACELEVEL = 2}. The JES Interface Level can be
 * configured by a mainframe administrator inside {@code FTP.DATA}.
 *
 * @see <a href=
 *      "https://www.ibm.com/support/knowledgecenter/en/SSLTBW_2.3.0/com.ibm.zos.v2r3.halu001/intfjes.htm">IBM
 *      Knowledge Center - Interfacing with JES</a>
 */
@Getter
public class JesClient implements Closeable {
	/**
	 * Wildcard value to be used for name and owner filters, meaning "any" value.
	 */
	public static final String FILTER_WILDCARD = constant("*");

	/**
	 * Maximum limit of spool entries (including)
	 */
	public static final int LIST_LIMIT_MAX = constant(1024);

	/**
	 * Limit of spool entries for {@link #exists(Job, JobStatus)}
	 *
	 * <p>
	 * Checking for existence does not need a limit, but using a limit allows to
	 * handle an additional error case.
	 */
	protected static final int LIST_LIMIT_EXISTS = constant(2);

	/**
	 * Minimum limit of spool entries (including)
	 */
	public static final int LIST_LIMIT_MIN = constant(1);

	/**
	 * Pattern to find the job ID inside the FTP response after submitting a JCL.
	 */
	private static final Pattern PATTERN_FTP_SUBMIT_ID = Pattern.compile("\\.\\s*(?<id>[^.]+?)\\s*$");

	/**
	 * Pattern to find the job name inside a valid JCL.
	 */
	private static final Pattern PATTERN_JCL_JOB_NAME = Pattern.compile("^//\\s*(?<name>\\S+)");

	/**
	 * Pattern to check the response string for the spool entries limit warning.
	 */
	private static final Pattern PATTERN_LIST_LIMIT
			= Pattern.compile("^250-JESENTRYLIMIT OF \\d+ REACHED\\.  ADDITIONAL ENTRIES NOT DISPLAYED$");

	/**
	 * Pattern to retrieve status values from response strings.
	 */
	private static final Pattern PATTERN_STATUS = Pattern
			.compile("^211-(SERVER SITE VARIABLE |TIMER )?(?<key>\\S+)( VALUE)? IS (SET TO )?(?<value>\\S+?)\\.?$");

	/**
	 * Default sleep interval used by {@link #waitFor(Job)}
	 */
	protected static final long SLEEP_DURATION_MILLIS_DEFAULT = constant(1000);

	/**
	 * FTP Client used by the current JES client instance.
	 *
	 * @return FTP client
	 */
	FTPClient ftpClient;

	/**
	 * Current JES spool user
	 *
	 * @return JES spool owner
	 */
	@NonFinal
	String jesOwner = FILTER_WILDCARD;

	/**
	 * Expert constructor. This constructor creates a FTP client <b>without</b>
	 * connecting and logging in. It is meant to be used in scenarios, which require
	 * additional FTP configuration.
	 *
	 * <p>
	 * <b>Usage example 1</b> (using a simplified login)
	 *
	 * <pre>
	 * // Construct the JES client and its internal FTP client
	 * try (JesClient jesClient = new JesClient()) {
	 *
	 *     // Connect via FTP
	 *     jesClient.getFtpClient().connect(...);
	 *
	 *     // Simplified login using the JES client
	 *     jesClient.login(...);
	 *
	 *     ...
	 *
	 * // Logout and disconnect using try-with-resource (close method)
	 * }
	 * </pre>
	 *
	 * <p>
	 * <b>Usage example 2:</b> (using a custom login)
	 *
	 * <pre>
	 * // Construct the JES client and its internal FTP client
	 * try (JesClient jesClient = new JesClient()) {
	 *
	 *     // Connect via FTP
	 *     jesClient.getFtpClient().connect(...);
	 *
	 *     // Login via FTP client
	 *     jesClient.getFtpClient().login(...);
	 *
	 *     // Set the JES spool owner
	 *     jesClient.setJesOwner(...);
	 *
	 *     // Enter JES mode of the FTP connection
	 *     jesClient.enterJesMode();
	 *
	 *     ...
	 *
	 * // Logout and disconnect using try-with-resource (close method)
	 * }
	 * </pre>
	 */
	public JesClient() {
		ftpClient = new FTPClient();
		getFtpClient().setParserFactory(new JesFtpFileEntryParserFactory());
	}

	/**
	 * Simplified constructor. This constructor initiates a new FTP connection and
	 * logs in using the given credentials.
	 *
	 * <p>
	 * The JesClient can store a JES spool owner. This constructor initializes the
	 * JES spool owner using the given username.
	 *
	 * @param hostname FTP hostname
	 * @param username FTP username and JES spool owner
	 * @param password FTP password
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public JesClient(final String hostname, final String username, final String password)
			throws IOException, JesException {
		this(hostname, FTPClient.DEFAULT_PORT, username, password);
	}

	/**
	 * Simplified constructor. This constructor initiates a new FTP connection and
	 * logs in using the given credentials.
	 *
	 * <p>
	 * The JesClient can store a JES spool owner. This constructor initializes the
	 * JES spool owner using the given username.
	 *
	 * @param hostname FTP hostname
	 * @param port     FTP port
	 * @param username FTP username and JES spool owner
	 * @param password FTP password
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public JesClient(final String hostname, final int port, final String username, final String password)
			throws IOException, JesException {
		this();
		getFtpClient().connect(hostname, port);
		login(username, password);
	}

	/**
	 * Logs out and disconnects the FTP connection.
	 */
	@Override
	public void close() throws IOException {
		try {
			if (getFtpClient().isAvailable()) {
				getFtpClient().logout();
			}
		} finally {
			if (getFtpClient().isConnected()) {
				getFtpClient().disconnect();
			}
		}
	}

	/**
	 * Removes a given {@code job} from JES spool. This method cares only about the
	 * jobs ID.
	 *
	 * <p>
	 * In case you do not already have a {@link Job} object, deleting by job ID
	 * works as follows:
	 *
	 * <pre>
	 * String jobId = ...;
	 * jesClient.delete(new Job(jobId, JesClient.FILTER_WILDCARD, JobStatus.ALL, JesClient.FILTER_WILDCARD));
	 * </pre>
	 *
	 * @param job Job to be deleted
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public void delete(final Job job) throws IOException, JesException {
		if (!getFtpClient().deleteFile(job.getId())) {
			throw new JesException(getFtpClient(), "Job [%s] could not be deleted.", job.getId());
		}
	}

	/**
	 * Enters the IBM z/OS FTP servers JES file type mode using a SITE command.
	 *
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public void enterJesMode() throws IOException, JesException {
		if (!getFtpClient().sendSiteCommand("FILEtype=JES")) {
			throw new JesException(getFtpClient(), "Failed setting JES mode.");
		}
	}

	/**
	 * Reloads the job from server and returns {@code true} if the job is still
	 * available and matches the given job status.
	 *
	 * @param job    the job to search for
	 * @param status job status or ALL
	 * @return {@code true} if the job is still available
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public boolean exists(final Job job, final JobStatus status) throws IOException, JesException {
		setJesFilters(job.getName(), status, job.getOwner(), LIST_LIMIT_EXISTS);

		final String[] ids = Optional.ofNullable(getFtpClient().listNames(job.getId()))
				.orElseThrow(() -> new JesException(getFtpClient(),
						"Retrieving job [%s] failed. Probably no FTP data connection socket could be opened.",
						job.getId()));
		return Optionals.ofSingle(ids).isPresent();
	}

	/**
	 * Retrieves up-to-date job details for {@code job}. That includes all
	 * {@link Job} attributes, including a list of {@link JobOutput} instances for
	 * held jobs.
	 *
	 * @param job job to get up-to-date details for
	 * @return job details or {@link Optional#empty()} in case the job is no longer
	 *         available inside JES spool
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public Optional<Job> getJobDetails(final Job job) throws IOException, JesException {
		setJesFilters(job.getName(), JobStatus.ALL, job.getOwner(), LIST_LIMIT_MAX);

		return Optionals.ofSingle(Arrays.stream(getFtpClient().listFiles(job.getId()))
				.filter(file -> file instanceof JesFtpFile)
				.map(file -> (JesFtpFile) file)
				.map(JesFtpFile::getJob));
	}

	/**
	 * Retrieves and parses a map of server properties, such as
	 * {@code "JESJOBNAME"}, {@code "JESSTATUS"}, {@code "JESOWNER"} and
	 * {@code "INTERFACELEVEL"}.
	 *
	 * @return map of server properties
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public Map<String, String> getServerProperties() throws IOException, JesException {
		// Execute STAT command
		if (!FTPReply.isPositiveCompletion(getFtpClient().stat())) {
			throw new JesException(getFtpClient(), "Failed executing STAT command.");
		}
		final String[] lines = getFtpClient().getReplyStrings();

		// Parse reply strings
		final Map<String, String> properties = new LinkedHashMap<>();
		for (final String line : lines) {
			final Optional<Matcher> matcher = Patterns.matches(PATTERN_STATUS, line);
			if (matcher.isPresent()) {
				// Key
				final String key = matcher.get().group("key");
				if (properties.containsKey(key)) {
					throw new JesException(key);
				}

				// Value
				properties.put(key, matcher.get().group("value"));
			}
		}
		return properties;
	}

	/**
	 * Returns a list of all job IDs boxed into {@link Job} objects matching the
	 * given filters. This method has a much higher performance compared to
	 * {@link #listFilled(String)}, though that method fills in additional
	 * {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} is allowed to end with the wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter filter by job names
	 * @return list of jobs containing job IDs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> list(final String nameFilter) throws IOException, JesException {
		return list(nameFilter, JobStatus.ALL);
	}

	/**
	 * Returns a list of all job IDs boxed into {@link Job} objects matching the
	 * given filters. This method has a much higher performance compared to
	 * {@link #listFilled(String, JobStatus)}, though that method fills in
	 * additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} is allowed to end with the wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter filter by job names
	 * @param status     filter by job status
	 * @return list of jobs containing job IDs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> list(final String nameFilter, final JobStatus status) throws IOException, JesException {
		return list(nameFilter, status, FILTER_WILDCARD);
	}

	/**
	 * Returns a list of all job IDs boxed into {@link Job} objects matching the
	 * given filters. This method has a much higher performance compared to
	 * {@link #listFilled(String, JobStatus, String)}, though that method fills in
	 * additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @return list of jobs containing job IDs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> list(final String nameFilter, final JobStatus status, final String ownerFilter)
			throws IOException, JesException {
		return list(nameFilter, status, ownerFilter, LIST_LIMIT_MAX);
	}

	/**
	 * Returns a list of all job IDs boxed into {@link Job} objects matching the
	 * given filters. This method has a much higher performance compared to
	 * {@link #listFilled(String, JobStatus, String, int)}, though that method fills
	 * in additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@code limit} entries. In case more entries are
	 * available, a {@link JesLimitReachedException} is thrown, containing all
	 * entries up to the limit. {@code limit} can be from {@link #LIST_LIMIT_MIN}
	 * (including) to {@link #LIST_LIMIT_MAX} (including).
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @param limit       limit of spool entries
	 * @return list of jobs containing job IDs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> list(final String nameFilter, final JobStatus status, final String ownerFilter, final int limit)
			throws IOException, JesException {
		setJesFilters(nameFilter, status, ownerFilter, limit);

		final String[] ids = Optional.ofNullable(getFtpClient().listNames())
				.orElseThrow(() -> new JesException(getFtpClient(),
						"Retrieving the list of job IDs failed. Probably no FTP data connection socket could be opened."));

		return throwIfLimitReached(limit,
				Arrays.stream(ids).map(id -> new Job(id, nameFilter, status, ownerFilter)).collect(toList()));
	}

	/**
	 * Returns a list of all {@link Job} objects matching the given filters. This
	 * method has a worse performance compared to {@link #list(String)}, though it
	 * fills in additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} is allowed to end with the wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter filter by job names
	 * @return list of jobs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> listFilled(final String nameFilter) throws IOException, JesException {
		return listFilled(nameFilter, JobStatus.ALL);
	}

	/**
	 * Returns a list of all {@link Job} objects matching the given filters. This
	 * method has a worse performance compared to {@link #list(String, JobStatus)},
	 * though it fills in additional {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} is allowed to end with the wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter filter by job names
	 * @param status     filter by job status
	 * @return list of jobs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> listFilled(final String nameFilter, final JobStatus status) throws IOException, JesException {
		return listFilled(nameFilter, status, FILTER_WILDCARD);
	}

	/**
	 * Returns a list of all {@link Job} objects matching the given filters. This
	 * method has a worse performance compared to
	 * {@link #list(String, JobStatus, String)}, though it fills in additional
	 * {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@link #LIST_LIMIT_MAX} entries. In case more
	 * entries are available, a {@link JesLimitReachedException} is thrown,
	 * containing all entries up to the limit.
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @return list of jobs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> listFilled(final String nameFilter, final JobStatus status, final String ownerFilter)
			throws IOException, JesException {
		return listFilled(nameFilter, status, ownerFilter, LIST_LIMIT_MAX);
	}

	/**
	 * Returns a list of all {@link Job} objects matching the given filters. This
	 * method has a worse performance compared to
	 * {@link #list(String, JobStatus, String)}, though it fills in additional
	 * {@link Job} fields.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * JES does not list more than {@code limit} entries. In case more entries are
	 * available, a {@link JesLimitReachedException} is thrown, containing all
	 * entries up to the limit. {@code limit} can be from {@link #LIST_LIMIT_MIN}
	 * (including) to {@link #LIST_LIMIT_MAX} (including).
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @param limit       limit of spool entries
	 * @return list of jobs
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public List<Job> listFilled(final String nameFilter,
			final JobStatus status,
			final String ownerFilter,
			final int limit) throws IOException, JesException {
		setJesFilters(nameFilter, status, ownerFilter, limit);

		final FTPFile[] files = getFtpClient().listFiles();

		return throwIfLimitReached(limit,
				Arrays.stream(files)
						.filter(file -> file instanceof JesFtpFile)
						.map(file -> (JesFtpFile) file)
						.map(JesFtpFile::getJob)
						.collect(toList()));
	}

	/**
	 * Shortcut method to perform a FTP login, set the internal JES owner and enter
	 * JES mode.
	 *
	 * <p>
	 * Is similar to the following lines of code.
	 *
	 * <pre>
	 * // Login via FTP client
	 * jesClient.getFtpClient().login(...);
	 *
	 * // Set the JES spool owner
	 * jesClient.setJesOwner(...);
	 *
	 * // Enter JES mode of the FTP connection
	 * jesClient.enterJesMode();
	 * </pre>
	 *
	 * @param username the user id to be used for FTP login and internal JES owner
	 * @param password the users password
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public void login(final String username, final String password) throws IOException, JesException {
		if (!getFtpClient().login(username, password)) {
			throw new JesException(getFtpClient(), "Could not login user [%s].", username);
		}
		setJesOwner(username);
		enterJesMode();
	}

	/**
	 * Retrieves the content of {@code jobOutput}.
	 *
	 * @param jobOutput job output to be requested
	 * @return content of the specified job output
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public String retrieve(final JobOutput jobOutput) throws IOException, JesException {
		final String fileName = Strings.format("%s.%d", jobOutput.getJob().getId(), jobOutput.getIndex());

		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			if (!getFtpClient().retrieveFile(fileName, outputStream)) {
				throw new JesException(getFtpClient(),
						"Could not retrieve data of job output [%s.%s].",
						jobOutput.getJob().getId(),
						jobOutput.getStep());
			}
			return outputStream.toString(Charset.defaultCharset().name());
		}
	}

	/**
	 * Retrieves all job outputs of {@code job}.
	 *
	 * @param job job to request all outputs of
	 * @return map with job output details and the corresponding content in specific
	 *         order
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public Map<JobOutput, String> retrieveOutputs(final Job job) throws IOException, JesException {
		if (job.getOutputs().isEmpty()) {
			return retrieveOutputs(
					getJobDetails(job).orElseThrow(() -> new JesException("Job [%s] is not available.", job.getId())));
		}
		return job.getOutputs()
				.stream()
				.collect(toLinkedHashMap(Function.identity(), ThrowingFunction.throwing(this::retrieve)));
	}

	/**
	 * Sends {@link org.apache.commons.net.ftp.FTPCmd#SITE} commands to set the
	 * given filter values.
	 *
	 * <p>
	 * {@code nameFilter} and {@code ownerFilter} are allowed to end with the
	 * wildcard character "*".
	 *
	 * <p>
	 * {@code limit} can be from {@link #LIST_LIMIT_MIN} (including) to
	 * {@link #LIST_LIMIT_MAX} (including). While that restriction is not checked by
	 * this method, values outside that range might result in a server side error
	 * message thrown as {@link JesException}.
	 *
	 * @param nameFilter  filter by job names
	 * @param status      filter by job status
	 * @param ownerFilter filter by job owner
	 * @param limit       limit of spool entries
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	protected void setJesFilters(final String nameFilter,
			final JobStatus status,
			final String ownerFilter,
			final int limit) throws IOException, JesException {
		if (!getFtpClient().sendSiteCommand("JESJOBName=" + nameFilter)) {
			throw new JesException(getFtpClient(), "Failed setting JES job name filter to [%s].", nameFilter);
		}
		if (!getFtpClient().sendSiteCommand("JESOwner=" + ownerFilter)) {
			throw new JesException(getFtpClient(), "Failed setting JES job owner filter to [%s].", ownerFilter);
		}
		if (!getFtpClient().sendSiteCommand("JESSTatus=" + status.getValue())) {
			throw new JesException(getFtpClient(), "Failed setting JES job status filter to [%s].", status.getValue());
		}
		if (!getFtpClient().sendSiteCommand("JESENTRYLIMIT=" + limit)) {
			throw new JesException(getFtpClient(),
					"Failed setting JES entry limit to %d. Minimum/Maximum: %d/%d",
					limit,
					LIST_LIMIT_MIN,
					LIST_LIMIT_MAX);
		}
	}

	/**
	 * Current JES spool user
	 *
	 * @param jesOwner JES spool owner
	 */
	public void setJesOwner(final String jesOwner) {
		this.jesOwner = Strings.toNeutralUpperCase(jesOwner).trim();
	}

	/**
	 * Submits the given JCL and returns a related {@link Job} object containing the
	 * started jobs ID.
	 *
	 * <p>
	 * In addition to the jobs ID this method tries to extract the jobs name from
	 * the given JCL. The returned owner is set to the internal JES owner, which can
	 * be set using {@link #setJesOwner(String)}.
	 *
	 * @param jclContent JCL to submit
	 * @return {@link Job} object containing the started jobs ID
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public Job submit(final String jclContent) throws IOException, JesException {
		try (InputStream inputStream = new ReaderInputStream(new StringReader(jclContent), Charset.defaultCharset())) {
			if (!getFtpClient().storeUniqueFile(inputStream)) {
				throw new JesException(getFtpClient(), "Submitting JCL failed.");
			}
		}

		final String id = Patterns.find(PATTERN_FTP_SUBMIT_ID, getFtpClient().getReplyString())
				.map(matcher -> matcher.group("id"))
				.orElseThrow(() -> new JesException(getFtpClient(), "Started job, but could not extract its ID."));
		final String name = Patterns.find(PATTERN_JCL_JOB_NAME, jclContent)
				.map(matcher -> matcher.group("name"))
				.orElse(FILTER_WILDCARD);

		return new Job(id, name, JobStatus.INPUT, getJesOwner());
	}

	/**
	 * In case the last FTP responses string contains the spool entries limit
	 * warning, a {@link JesLimitReachedException} is thrown, else {@code jobs} are
	 * returned.
	 *
	 * <p>
	 * The thrown exception contains the current spool entries limit and all
	 * entries, which were read already.
	 *
	 * @param limit current spool entries limit
	 * @param jobs  list of jobs
	 * @return {@code jobs} in case the spool entries limit is not reached
	 * @throws JesLimitReachedException if the last FTP responses string contains
	 *                                  the spool entries limit warning
	 */
	protected List<Job> throwIfLimitReached(final int limit, final List<Job> jobs) throws JesLimitReachedException {
		if (Patterns.find(PATTERN_LIST_LIMIT, getFtpClient().getReplyString()).isPresent()) {
			throw new JesLimitReachedException(limit, jobs, getFtpClient());
		}
		return jobs;
	}

	/**
	 * Waits for {@code job} to be finished using {@code Thread#sleep} for sleeping
	 * between {@link #exists(Job, JobStatus)} calls and not timing out. The sleep
	 * duration is {@link #SLEEP_DURATION_MILLIS_DEFAULT}.
	 *
	 * <p>
	 * The given jobs status specifies, which status are waited for:
	 * <ul>
	 * <li>{@link JobStatus#ALL}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#INPUT}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#ACTIVE}: waiting for {@link JobStatus#ACTIVE} only
	 * <li>{@link JobStatus#OUTPUT}: returning {@code true} with no checks and
	 * without sleeping
	 * </ul>
	 *
	 * @param job the job to wait for
	 * @throws InterruptedException if any thread has interrupted the current thread
	 * @throws IOException          Technical FTP failure
	 * @throws JesException         Logical JES failure
	 */
	public void waitFor(final Job job) throws InterruptedException, IOException, JesException {
		waitFor(job, SLEEP_DURATION_MILLIS_DEFAULT);
	}

	/**
	 * Waits for {@code job} to be finished using {@code Thread#sleep} for sleeping
	 * between {@link #exists(Job, JobStatus)} calls and not timing out.
	 * {@code sleepDurationMillis} allows to specify the milliseconds to sleep.
	 *
	 * <p>
	 * The given jobs status specifies, which status are waited for:
	 * <ul>
	 * <li>{@link JobStatus#ALL}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#INPUT}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#ACTIVE}: waiting for {@link JobStatus#ACTIVE} only
	 * <li>{@link JobStatus#OUTPUT}: returning {@code true} with no checks and
	 * without sleeping
	 * </ul>
	 *
	 * @param job                 the job to wait for
	 * @param sleepDurationMillis milliseconds to sleep
	 * @throws InterruptedException if any thread has interrupted the current thread
	 * @throws IOException          Technical FTP failure
	 * @throws JesException         Logical JES failure
	 */
	public void waitFor(final Job job, final long sleepDurationMillis)
			throws InterruptedException, IOException, JesException {
		if (!waitFor(job, sleepDurationMillis, Long.MAX_VALUE)) {
			throw new JesException(
					"JesClient.waitFor(%s, %d, Long.MAX_VALUE) returned false while it should have waited forever.",
					job.getId(),
					sleepDurationMillis);
		}
	}

	/**
	 * Waits for {@code job} to be finished using {@code Thread#sleep} for sleeping
	 * between {@link #exists(Job, JobStatus)} calls and timing out after a given
	 * amount of milliseconds. {@code sleepDurationMillis} allows to specify the
	 * milliseconds to sleep.
	 *
	 * <p>
	 * The given jobs status specifies, which status are waited for:
	 * <ul>
	 * <li>{@link JobStatus#ALL}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#INPUT}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#ACTIVE}: waiting for {@link JobStatus#ACTIVE} only
	 * <li>{@link JobStatus#OUTPUT}: returning {@code true} with no checks and
	 * without sleeping
	 * </ul>
	 *
	 * @param job                 the job to wait for
	 * @param sleepDurationMillis milliseconds to sleep
	 * @param timeoutMillis       timeout in milliseconds
	 * @return {@code true} if the job finished and {@code false} if the timeout has
	 *         been reached
	 * @throws InterruptedException if any thread has interrupted the current thread
	 * @throws IOException          Technical FTP failure
	 * @throws JesException         Logical JES failure
	 */
	@SuppressWarnings("unused")
	public boolean waitFor(final Job job, final long sleepDurationMillis, final long timeoutMillis)
			throws InterruptedException, IOException, JesException {
		return waitFor(job, ThrowingRunnable.throwing(() -> Thread.sleep(sleepDurationMillis)), timeoutMillis);
	}

	/**
	 * Waits for {@code job} to be finished using {@code sleep} for sleeping between
	 * {@link #exists(Job, JobStatus)} calls and not timing out.
	 *
	 * <p>
	 * The given jobs status specifies, which status are waited for:
	 * <ul>
	 * <li>{@link JobStatus#ALL}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#INPUT}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#ACTIVE}: waiting for {@link JobStatus#ACTIVE} only
	 * <li>{@link JobStatus#OUTPUT}: returning {@code true} with no checks and
	 * without sleeping
	 * </ul>
	 *
	 * @param job   the job to wait for
	 * @param sleep method to use for sleeping
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public void waitFor(final Job job, final Runnable sleep) throws IOException, JesException {
		if (!waitFor(job, sleep, Long.MAX_VALUE)) {
			throw new JesException(
					"JesClient.waitFor(%s, Runnable, Long.MAX_VALUE) returned false while it should have waited forever.",
					job.getId());
		}
	}

	/**
	 * Waits for {@code job} to be finished using {@code sleep} for sleeping between
	 * {@link #exists(Job, JobStatus)} calls and timing out after a given amount of
	 * milliseconds.
	 *
	 * <p>
	 * The given jobs status specifies, which status are waited for:
	 * <ul>
	 * <li>{@link JobStatus#ALL}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#INPUT}: waiting for {@link JobStatus#INPUT} and
	 * {@link JobStatus#ACTIVE}
	 * <li>{@link JobStatus#ACTIVE}: waiting for {@link JobStatus#ACTIVE} only
	 * <li>{@link JobStatus#OUTPUT}: returning {@code true} with no checks and
	 * without sleeping
	 * </ul>
	 *
	 * @param job           the job to wait for
	 * @param sleep         method to use for sleeping
	 * @param timeoutMillis timeout in milliseconds
	 * @return {@code true} if the job finished and {@code false} if the timeout has
	 *         been reached
	 * @throws IOException  Technical FTP failure
	 * @throws JesException Logical JES failure
	 */
	public boolean waitFor(final Job job, final Runnable sleep, final long timeoutMillis)
			throws IOException, JesException {
		// Early exits
		if (job.getStatus() == JobStatus.OUTPUT) {
			return true;
		}
		if (timeoutMillis <= 0) {
			return false;
		}

		// Status INPUT and ACTIVE might need to be waited for.
		final List<JobStatus> stati;
		if (job.getStatus() == JobStatus.ACTIVE) {
			stati = Arrays.asList(JobStatus.ACTIVE);
		} else {
			stati = Arrays.asList(JobStatus.INPUT, JobStatus.ACTIVE);
		}

		// Sleep and check for timeout
		final long startTimeMillis = System.currentTimeMillis();
		for (final JobStatus status : stati) {
			while (exists(job, status)) {
				if (startTimeMillis + timeoutMillis <= System.currentTimeMillis()) {
					return false;
				}
				sleep.run();
				if (startTimeMillis + timeoutMillis <= System.currentTimeMillis()) {
					return false;
				}
			}
		}

		return true;
	}
}
