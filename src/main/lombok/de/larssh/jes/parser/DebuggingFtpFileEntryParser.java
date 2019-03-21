package de.larssh.jes.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileEntryParser;

import de.larssh.utils.text.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import lombok.NoArgsConstructor;

/**
 * Implementation of {@link org.apache.commons.net.ftp.FTPFileEntryParser}
 * throwing at each method to simplify debugging wrapped FTP connections
 */
@NoArgsConstructor
public class DebuggingFtpFileEntryParser implements FTPFileEntryParser {
	/** {@inheritDoc} */
	@Nullable
	@Override
	public FTPFile parseFTPEntry(@Nullable final String listEntry) {
		if (listEntry == null) {
			throw new IllegalArgumentException("listEntry");
		}
		throw new DebuggingFtpFileEntryParserException("parseFTPEntry", listEntry);
	}

	/** {@inheritDoc} */
	@Nullable
	@Override
	public String readNextEntry(@Nullable final BufferedReader reader) throws IOException {
		if (reader == null) {
			throw new IllegalArgumentException("reader");
		}
		return reader.readLine();
	}

	/** {@inheritDoc} */
	@NonNull
	@Override
	public List<String> preParse(@Nullable final List<String> original) {
		if (original == null) {
			throw new IllegalArgumentException("original");
		}
		throw new DebuggingFtpFileEntryParserException("preParse", String.join(Strings.NEW_LINE, original));
	}
}
