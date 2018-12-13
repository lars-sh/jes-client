package de.larssh.jes.parser;

import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFileEntryParser;
import org.apache.commons.net.ftp.parser.FTPFileEntryParserFactory;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import lombok.NoArgsConstructor;

/**
 * Factory implementation to create implementations of
 * {@link JesFtpFileEntryParser}.
 */
@NoArgsConstructor
public class JesFtpFileEntryParserFactory implements FTPFileEntryParserFactory {
	/** {@inheritDoc} */
	@NonNull
	@Override
	public FTPFileEntryParser createFileEntryParser(@SuppressWarnings("unused") @Nullable final String key) {
		return new JesFtpFileEntryParser();
	}

	/** {@inheritDoc} */
	@NonNull
	@Override
	public FTPFileEntryParser createFileEntryParser(
			@SuppressWarnings("unused") @Nullable final FTPClientConfig config) {
		return new JesFtpFileEntryParser();
	}
}
