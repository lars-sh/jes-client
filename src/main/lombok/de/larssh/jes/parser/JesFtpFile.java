package de.larssh.jes.parser;

import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.net.ftp.FTPFile;

import de.larssh.jes.Job;
import de.larssh.utils.annotations.SuppressJacocoGenerated;
import lombok.Getter;
import lombok.ToString;

/**
 * {@link FTPFile} implementation to hold a {@link Job} object.
 */
@Getter
@ToString(callSuper = false)
public class JesFtpFile extends FTPFile {

	private static final long serialVersionUID = 6881059052088614926L;

	/**
	 * Job details
	 *
	 * @return job details
	 */
	transient Job job;

	/**
	 * Creates a file containing a {@link Job} object and sets the original FTP
	 * server raw listing from which the job was created.
	 *
	 * @param job        job details
	 * @param rawListing raw FTP server listing
	 */
	@SuppressWarnings("PMD.CallSuperInConstructor")
	public JesFtpFile(final Job job, final String rawListing) {
		this.job = job;
		setRawListing(rawListing);
	}

	/**
	 * This class cannot be deserialized.
	 *
	 * @param stream object input stream
	 * @throws NotSerializableException This class cannot be deserialized.
	 */
	@SuppressJacocoGenerated(justification = "this is not serializable")
	private void readObject(@SuppressWarnings("unused") final ObjectInputStream stream)
			throws NotSerializableException {
		throw new NotSerializableException(JesFtpFile.class.getName());
	}

	/**
	 * This class cannot be serialized.
	 *
	 * @param stream object output stream
	 * @throws NotSerializableException This class cannot be serialized.
	 */
	@SuppressJacocoGenerated(justification = "this is not serializable")
	private void writeObject(@SuppressWarnings("unused") final ObjectOutputStream stream)
			throws NotSerializableException {
		throw new NotSerializableException(JesFtpFile.class.getName());
	}
}
