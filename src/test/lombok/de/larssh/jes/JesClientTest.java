package de.larssh.jes;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.joor.Reflect;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.Invocation;

import de.larssh.jes.parser.JesFtpFile;
import de.larssh.utils.Nullables;
import de.larssh.utils.SneakyException;
import de.larssh.utils.collection.Maps;
import de.larssh.utils.function.ThrowingConsumer;
import de.larssh.utils.test.Reflects;
import de.larssh.utils.time.Stopwatch;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.NoArgsConstructor;
import lombok.experimental.NonFinal;

/**
 * {@link JesClient}
 */
@NoArgsConstructor
@SuppressWarnings({
		"checkstyle:SuppressWarnings",
		"PMD.ExcessiveClassLength",
		"PMD.ExcessiveImports",
		"PMD.ExcessiveMethodLength",
		"PMD.NcssCount",
		"resource" })
public class JesClientTest {
	private static final Job TEST_DATA_JOB = new Job("id", "name", JobStatus.OUTPUT, "owner");

	/**
	 * Verify and clears a mocked JES client and its inner mocks for no more
	 * interactions.
	 *
	 * @param jesClient mocked JES Client to be verified
	 */
	private static void verifyEnd(final MockedJesClient jesClient) {
		for (final Invocation invocation : mockingDetails(jesClient).getInvocations()) {
			if (invocation.getMethod().getDeclaringClass() == MockedJesClient.class) {
				invocation.ignoreForVerification();
			}
		}

		verifyNoMoreInteractions(jesClient);
		verifyNoMoreInteractions(jesClient.getFtpClient());
		clearInvocations(jesClient, jesClient.getFtpClient());
	}

	/**
	 * Verify and clears a mocked JES client and its inner mocks for no more
	 * interactions. The last performed mocked action is expected to be the creation
	 * of a {@link JesException}.
	 *
	 * @param jesClient mocked JES Client to be verified
	 */
	private static void verifyEndWithJesException(final MockedJesClient jesClient) {
		verify(jesClient.getFtpClient()).getReplyString();
		verifyEnd(jesClient);
	}

	/**
	 * {@link JesClient#close()}
	 */
	@Test
	public void testClose() {
		// given
		try {
			final MockedJesClient jesClient = MockedJesClient.newInstance();
			when(jesClient.getFtpClient().isAvailable()).thenReturn(true);
			when(jesClient.getFtpClient().isConnected()).thenReturn(true);

			// when
			jesClient.close();

			// then
			verify(jesClient).close();
			verify(jesClient.getFtpClient()).isAvailable();
			verify(jesClient.getFtpClient()).logout();
			verify(jesClient.getFtpClient()).isConnected();
			verify(jesClient.getFtpClient()).disconnect();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}

		// given
		try {
			final MockedJesClient jesClient = MockedJesClient.newInstance();
			when(jesClient.getFtpClient().isAvailable()).thenReturn(false);
			when(jesClient.getFtpClient().isConnected()).thenReturn(true);

			// when
			jesClient.close();

			// then
			verify(jesClient).close();
			verify(jesClient.getFtpClient()).isAvailable();
			verify(jesClient.getFtpClient()).isConnected();
			verify(jesClient.getFtpClient()).disconnect();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}

		// given
		try {
			final MockedJesClient jesClient = MockedJesClient.newInstance();
			when(jesClient.getFtpClient().isAvailable()).thenReturn(false);
			when(jesClient.getFtpClient().isConnected()).thenReturn(false);

			// when
			jesClient.close();

			// then
			verify(jesClient).close();
			verify(jesClient.getFtpClient()).isAvailable();
			verify(jesClient.getFtpClient()).isConnected();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * {@link JesClient#delete(Job)}
	 */
	@Test
	public void testDelete() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().deleteFile(any())).thenReturn(true);

			// when
			jesClient.delete(TEST_DATA_JOB);

			// then
			verify(jesClient).delete(any());
			verify(jesClient.getFtpClient()).deleteFile("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().deleteFile(any())).thenReturn(false);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.delete(TEST_DATA_JOB));

			// then
			verify(jesClient).delete(any());
			verify(jesClient.getFtpClient()).deleteFile("ID");
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#enterJesMode()}
	 */
	@Test
	public void testEnterJesMode() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(any())).thenReturn(true);

			// when
			jesClient.enterJesMode();

			// then
			verify(jesClient).enterJesMode();
			verify(jesClient.getFtpClient()).sendSiteCommand("FILEtype=JES");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(any())).thenReturn(false);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.enterJesMode());

			// then
			verify(jesClient).enterJesMode();
			verify(jesClient.getFtpClient()).sendSiteCommand("FILEtype=JES");
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#exists(Job, JobStatus)}
	 */
	@Test
	public void testExists() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames(any())).thenReturn(new String[0]);

			// when
			assertThat(jesClient.exists(TEST_DATA_JOB, JobStatus.INPUT)).isFalse();

			// then
			verify(jesClient).exists(any(), any());
			verify(jesClient).setJesFilters("NAME", JobStatus.INPUT, "OWNER", 2);
			verify(jesClient.getFtpClient()).listNames("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames(any())).thenReturn(new String[] { "ID" });

			// when
			assertThat(jesClient.exists(TEST_DATA_JOB, JobStatus.INPUT)).isTrue();

			// then
			verify(jesClient).exists(any(), any());
			verify(jesClient).setJesFilters("NAME", JobStatus.INPUT, "OWNER", 2);
			verify(jesClient.getFtpClient()).listNames("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames(any())).thenReturn(null);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("");

			// when
			assertThatExceptionOfType(JesException.class)
					.isThrownBy(() -> jesClient.exists(TEST_DATA_JOB, JobStatus.INPUT));

			// then
			verify(jesClient).exists(any(), any());
			verify(jesClient).setJesFilters("NAME", JobStatus.INPUT, "OWNER", 2);
			verify(jesClient.getFtpClient()).listNames("ID");
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames(any())).thenReturn(null);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("550 NO JOBS FOUND FOR ...");

			// when
			assertThat(jesClient.exists(TEST_DATA_JOB, JobStatus.INPUT)).isFalse();

			// then
			verify(jesClient).exists(any(), any());
			verify(jesClient).setJesFilters("NAME", JobStatus.INPUT, "OWNER", 2);
			verify(jesClient.getFtpClient()).listNames("ID");
			verify(jesClient.getFtpClient()).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#getJesOwner()} and {@link JesClient#setJesOwner(String)}
	 */
	@Test
	@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
			justification = "Return value of Mockedverify(jesClient).getJesOwner() does not matter.")
	public void testGetAndSetJesOwner() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			// when 1
			assertThat("*").isEqualTo(jesClient.getJesOwner());

			// then 1
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);

			// when 2
			jesClient.setJesOwner("jesOwner1");
			assertThat("JESOWNER1").isEqualTo(jesClient.getJesOwner());

			// then 2
			verify(jesClient).setJesOwner(any());
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);

			// when 3
			jesClient.setJesOwner(" jesOwner2 ");
			assertThat("JESOWNER2").isEqualTo(jesClient.getJesOwner());

			// then 3
			verify(jesClient).setJesOwner(any());
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * {@link JesClient#getJobDetails(Job)}
	 */
	@Test
	public void testGetJobDetails() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles(any()))
					.thenReturn(new JesFtpFile[] { new JesFtpFile(TEST_DATA_JOB, "") });

			// when
			assertThat(Optional.of(TEST_DATA_JOB)).isEqualTo(jesClient.getJobDetails(TEST_DATA_JOB));

			// then
			verify(jesClient).getJobDetails(any());
			verify(jesClient).setJesFilters("NAME", JobStatus.ALL, "OWNER", 1024);
			verify(jesClient.getFtpClient()).listFiles("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles(any())).thenReturn(new JesFtpFile[0]);

			// when
			assertThat(Optional.empty()).isEqualTo(jesClient.getJobDetails(TEST_DATA_JOB));

			// then
			verify(jesClient).getJobDetails(any());
			verify(jesClient).setJesFilters("NAME", JobStatus.ALL, "OWNER", 1024);
			verify(jesClient.getFtpClient()).listFiles("ID");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#getServerProperties()}
	 */
	@Test
	public void testGetServerProperties() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_OK);
			when(jesClient.getFtpClient().getReplyStrings()).thenReturn(new String[0]);

			// when
			assertThat(emptyMap()).isEqualTo(jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verify(jesClient.getFtpClient()).getReplyStrings();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_OK);
			when(jesClient.getFtpClient().getReplyStrings()).thenReturn(new String[] { "abc", "211-a IS b", "def" });

			// when
			final Map<String, String> properties = Maps.builder(new LinkedHashMap<String, String>()) //
					.put("a", "b")
					.get();
			assertThat(properties).isEqualTo(jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verify(jesClient.getFtpClient()).getReplyStrings();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_OK);
			when(jesClient.getFtpClient().getReplyStrings()).thenReturn(new String[] { "211-a IS b", "211-c IS d" });

			// when
			final Map<String, String> properties = Maps.builder(new LinkedHashMap<String, String>()) //
					.put("a", "b")
					.put("c", "d")
					.get();
			assertThat(properties).isEqualTo(jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verify(jesClient.getFtpClient()).getReplyStrings();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_OK);
			when(jesClient.getFtpClient().getReplyStrings()).thenReturn(new String[] { "211-a IS b", "211-a IS d" });

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verify(jesClient.getFtpClient()).getReplyStrings();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().stat()).thenReturn(FTPReply.COMMAND_NOT_IMPLEMENTED);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.getServerProperties());

			// then
			verify(jesClient).getServerProperties();
			verify(jesClient.getFtpClient()).stat();
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#list(String)}, {@link JesClient#list(String, JobStatus)},
	 * {@link JesClient#list(String, JobStatus, String)} and
	 * {@link JesClient#list(String, JobStatus, String, int)}
	 */
	@Test
	public void testList() {
		final String nameFilter = "nameFilter";
		final JobStatus status = JobStatus.OUTPUT;
		final String ownerFilter = "ownerFilter";
		final int limit = 123;
		final List<Job> jobs = asList(TEST_DATA_JOB);

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).list(any(), any());

			// when
			assertThat(jobs).isEqualTo(jesClient.list(nameFilter));

			// then
			verify(jesClient).list(any());
			verify(jesClient).list(nameFilter, JobStatus.ALL);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).list(any(), any(), any());

			// when
			assertThat(jobs).isEqualTo(jesClient.list(nameFilter, status));

			// then
			verify(jesClient).list(any(), any());
			verify(jesClient).list(nameFilter, status, "*");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).list(any(), any(), any(), anyInt());

			// when
			assertThat(jobs).isEqualTo(jesClient.list(nameFilter, status, ownerFilter));

			// then
			verify(jesClient).list(any(), any(), any());
			verify(jesClient).list(nameFilter, status, ownerFilter, 1024);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(new String[] { "id", "id" });
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertThat(asList(TEST_DATA_JOB, TEST_DATA_JOB))
					.isEqualTo(jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient).throwIfLimitReached(limit, asList(TEST_DATA_JOB, TEST_DATA_JOB));
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(new String[] { "id" });
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertThat(jobs).isEqualTo(jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient).throwIfLimitReached(limit, jobs);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(new String[0]);
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertThat(emptyList()).isEqualTo(jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient).throwIfLimitReached(limit, emptyList());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(null);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("");

			// when
			assertThatExceptionOfType(JesException.class)
					.isThrownBy(() -> jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listNames()).thenReturn(null);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("550 NO JOBS FOUND FOR ...");

			// when
			assertThat(emptyList()).isEqualTo(jesClient.list(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).list(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listNames();
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verify(jesClient).throwIfLimitReached(limit, emptyList());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#listFilled(String)},
	 * {@link JesClient#listFilled(String, JobStatus)},
	 * {@link JesClient#listFilled(String, JobStatus, String)} and
	 * {@link JesClient#listFilled(String, JobStatus, String, int)}
	 */
	@Test
	public void testListFilled() {
		final String nameFilter = "nameFilter";
		final JobStatus status = JobStatus.OUTPUT;
		final String ownerFilter = "ownerFilter";
		final int limit = 123;
		final List<Job> jobs = asList(TEST_DATA_JOB);

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).listFilled(any(), any());

			// when
			assertThat(jobs).isEqualTo(jesClient.listFilled(nameFilter));

			// then
			verify(jesClient).listFilled(any());
			verify(jesClient).listFilled(nameFilter, JobStatus.ALL);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).listFilled(any(), any(), any());

			// when
			assertThat(jobs).isEqualTo(jesClient.listFilled(nameFilter, status));

			// then
			verify(jesClient).listFilled(any(), any());
			verify(jesClient).listFilled(nameFilter, status, "*");
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(jobs).when(jesClient).listFilled(any(), any(), any(), anyInt());

			// when
			assertThat(jobs).isEqualTo(jesClient.listFilled(nameFilter, status, ownerFilter));

			// then
			verify(jesClient).listFilled(any(), any(), any());
			verify(jesClient).listFilled(nameFilter, status, ownerFilter, 1024);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles())
					.thenReturn(new JesFtpFile[] { new JesFtpFile(TEST_DATA_JOB, "") });
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertThat(jobs).isEqualTo(jesClient.listFilled(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).listFilled(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listFiles();
			verify(jesClient).throwIfLimitReached(limit, jobs);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles()).thenReturn(
					new JesFtpFile[] { new JesFtpFile(TEST_DATA_JOB, ""), new JesFtpFile(TEST_DATA_JOB, "") });
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertThat(asList(TEST_DATA_JOB, TEST_DATA_JOB))
					.isEqualTo(jesClient.listFilled(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).listFilled(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listFiles();
			verify(jesClient).throwIfLimitReached(limit, asList(TEST_DATA_JOB, TEST_DATA_JOB));
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doNothing().when(jesClient).setJesFilters(any(), any(), any(), anyInt());
			when(jesClient.getFtpClient().listFiles()).thenReturn(new JesFtpFile[0]);
			doAnswer(invocation -> invocation.getArgument(1)).when(jesClient).throwIfLimitReached(anyInt(), any());

			// when
			assertThat(emptyList()).isEqualTo(jesClient.listFilled(nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).listFilled(any(), any(), any(), anyInt());
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).listFiles();
			verify(jesClient).throwIfLimitReached(limit, emptyList());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#login(String, String)}
	 */
	@Test
	public void testLogin() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().login(any(), any())).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(any())).thenReturn(true);
			doNothing().when(jesClient).enterJesMode();
			doNothing().when(jesClient).setJesOwner(any());

			// when
			jesClient.login("username", "password");

			// then
			verify(jesClient).login(any(), any());
			verify(jesClient.getFtpClient()).login("username", "password");
			verify(jesClient).setJesOwner("username");
			verify(jesClient).enterJesMode();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().login(any(), any())).thenReturn(false);
			when(jesClient.getFtpClient().sendSiteCommand(any())).thenReturn(true);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.login("username", "password"));

			// then
			verify(jesClient).login(any(), any());
			verify(jesClient.getFtpClient()).login("username", "password");
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#retrieve(JobOutput)}
	 */
	@Test
	public void testRetrieve() {
		final Job job = new Job("id", "name", JobStatus.OUTPUT, "owner");
		final JobOutput jobOutput1
				= job.createOutput(1, "outputName", 10, Optional.empty(), Optional.empty(), Optional.empty());
		final JobOutput jobOutput2
				= job.createOutput(2, "outputName", 10, Optional.of("step"), Optional.empty(), Optional.empty());

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().retrieveFile(any(), any())).then(in -> {
				((OutputStream) in.getArgument(1)).write("jobOutput1".getBytes(Charset.defaultCharset()));
				return true;
			});

			// when
			assertThat("jobOutput1").isEqualTo(jesClient.retrieve(jobOutput1));

			// then
			verify(jesClient).retrieve(any());
			verify(jesClient.getFtpClient()).retrieveFile(eq("ID.1"), any());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().retrieveFile(any(), any())).then(in -> {
				((OutputStream) in.getArgument(1)).write("jobOutput2".getBytes(Charset.defaultCharset()));
				return true;
			});

			// when
			assertThat("jobOutput2").isEqualTo(jesClient.retrieve(jobOutput2));

			// then
			verify(jesClient).retrieve(any());
			verify(jesClient.getFtpClient()).retrieveFile(eq("ID.2"), any());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().retrieveFile(any(), any())).thenReturn(false);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.retrieve(jobOutput1));

			// then
			verify(jesClient).retrieve(any());
			verify(jesClient.getFtpClient()).retrieveFile(eq("ID.1"), any());
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#retrieveOutputs(Job)}
	 */
	@Test
	public void testRetrieveOutputs() {
		final Job job = new Job("id", "name", JobStatus.OUTPUT, "owner");
		final JobOutput jobOutput1
				= job.createOutput(1, "outputName", 10, Optional.empty(), Optional.empty(), Optional.empty());
		final JobOutput jobOutput2
				= job.createOutput(2, "outputName", 10, Optional.of("step"), Optional.empty(), Optional.empty());

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn("jobOutput1").when(jesClient).retrieve(jobOutput1);
			doReturn("jobOutput2").when(jesClient).retrieve(jobOutput2);

			// when
			final Map<JobOutput, String> jobOutputs = Maps.builder(new LinkedHashMap<JobOutput, String>())
					.put(jobOutput1, "jobOutput1")
					.put(jobOutput2, "jobOutput2")
					.get();
			assertThat(jobOutputs).isEqualTo(jesClient.retrieveOutputs(job));

			// then
			verify(jesClient).retrieveOutputs(any());
			verify(jesClient).retrieve(jobOutput1);
			verify(jesClient).retrieve(jobOutput2);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(Optional.of(job)).when(jesClient).getJobDetails(any());
			doReturn("jobOutput1").when(jesClient).retrieve(jobOutput1);
			doReturn("jobOutput2").when(jesClient).retrieve(jobOutput2);

			// when
			final Map<JobOutput, String> jobOutputs = Maps.builder(new LinkedHashMap<JobOutput, String>())
					.put(jobOutput1, "jobOutput1")
					.put(jobOutput2, "jobOutput2")
					.get();
			assertThat(jobOutputs).isEqualTo(jesClient.retrieveOutputs(TEST_DATA_JOB));

			// then
			verify(jesClient, times(2)).retrieveOutputs(any());
			verify(jesClient).getJobDetails(TEST_DATA_JOB);
			verify(jesClient).retrieve(jobOutput1);
			verify(jesClient).retrieve(jobOutput2);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(Optional.empty()).when(jesClient).getJobDetails(any());

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.retrieveOutputs(TEST_DATA_JOB));

			// then
			verify(jesClient).retrieveOutputs(any());
			verify(jesClient).getJobDetails(TEST_DATA_JOB);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#setJesFilters(String, JobStatus, String, int)}
	 */
	@Test
	@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_INFERRED",
			justification = "Return value of Reflect.on(JesClient).call(\"setJesFilters\", ...) is of type void.")
	public void testSetJesFilters() {
		final String nameFilter = "nameFilter";
		final String ownerFilter = "ownerFilter";
		final JobStatus status = JobStatus.OUTPUT;
		final int limit = 123;

		final String siteCommandNameFilter = "JESJOBName=" + nameFilter;
		final String siteCommandOwnerFilter = "JESOwner=" + ownerFilter;
		final String siteCommandStatusFilter = "JESSTatus=" + status.getValue();
		final String siteCommandLimitFilter = "JESENTRYLIMIT=" + Integer.toString(limit);

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandOwnerFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandStatusFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandLimitFilter)).thenReturn(true);

			// when
			Reflect.on(jesClient).call("setJesFilters", nameFilter, status, ownerFilter, limit);

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandOwnerFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandStatusFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandLimitFilter);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandOwnerFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandStatusFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandLimitFilter)).thenReturn(false);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> Reflects
					.call(Reflect.on(jesClient), "setJesFilters", nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandOwnerFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandStatusFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandLimitFilter);
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandOwnerFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandStatusFilter)).thenReturn(false);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> Reflects
					.call(Reflect.on(jesClient), "setJesFilters", nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandOwnerFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandStatusFilter);
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(true);
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandOwnerFilter)).thenReturn(false);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> Reflects
					.call(Reflect.on(jesClient), "setJesFilters", nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandOwnerFilter);
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().sendSiteCommand(siteCommandNameFilter)).thenReturn(false);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> Reflects
					.call(Reflect.on(jesClient), "setJesFilters", nameFilter, status, ownerFilter, limit));

			// then
			verify(jesClient).setJesFilters(nameFilter, status, ownerFilter, limit);
			verify(jesClient.getFtpClient()).sendSiteCommand(siteCommandNameFilter);
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#submit(String)}
	 */
	@Test
	@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
			justification = "Return value of Mockedverify(jesClient).getJesOwner() does not matter.")
	public void testSubmit() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().storeUniqueFile(any(), any())).then(invocation -> {
				try (InputStream inputStream = invocation.getArgument(1)) {
					assertThat("jclContent").isEqualTo(IOUtils.toString(inputStream, Charset.defaultCharset()));
				}
				return true;
			});
			when(jesClient.getFtpClient().getReplyString()).thenReturn("250-IT IS KNOWN TO JES AS id");

			// when
			assertThat(new Job("id", "*", JobStatus.INPUT, "*")).isEqualTo(jesClient.submit("jclContent"));

			// then
			verify(jesClient).submit(any());
			verify(jesClient.getFtpClient()).storeUniqueFile(any(), any());
			verify(jesClient.getFtpClient()).getReplyString();
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().storeUniqueFile(any(), any())).thenReturn(true);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("250-IT IS KNOWN TO JES AS id");

			// when
			assertThat(new Job("id", "name", JobStatus.INPUT, "*")).isEqualTo(jesClient.submit("//name\njclContent"));

			// then
			verify(jesClient).submit(any());
			verify(jesClient.getFtpClient()).storeUniqueFile(any(), any());
			verify(jesClient.getFtpClient()).getReplyString();
			verify(jesClient).getJesOwner();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().storeUniqueFile(any(), any())).thenReturn(false);

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.submit("jclContent"));

			// then
			verify(jesClient).submit(any());
			verify(jesClient.getFtpClient()).storeUniqueFile(any(), any());
			verifyEndWithJesException(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().storeUniqueFile(any(), any())).thenReturn(true);
			when(jesClient.getFtpClient().getReplyString()).thenReturn("");

			// when
			assertThatExceptionOfType(JesException.class).isThrownBy(() -> jesClient.submit("jclContent"));

			// then
			verify(jesClient).submit(any());
			verify(jesClient.getFtpClient()).storeUniqueFile(any(), any());
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#throwIfLimitReached(int, List)}
	 */
	@Test
	@SuppressWarnings("checkstyle:XStringMatches")
	public void testThrowIfLimitReached() {
		final int limit = 123;
		final List<Job> jobs = asList(TEST_DATA_JOB);

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().getReplyString()).thenReturn("");

			// when
			assertThat(jobs).isEqualTo(Reflect.on(jesClient).call("throwIfLimitReached", limit, jobs).get());

			// then
			verify(jesClient).throwIfLimitReached(limit, jobs);
			verify(jesClient.getFtpClient()).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			when(jesClient.getFtpClient().getReplyString())
					.thenReturn("250-JESENTRYLIMIT OF 456 REACHED.  ADDITIONAL ENTRIES NOT DISPLAYED");

			// when
			assertThatExceptionOfType(JesLimitReachedException.class)
					.isThrownBy(() -> Reflects.call(Reflect.on(jesClient), "throwIfLimitReached", limit, jobs))
					.matches(exception -> exception.getLimit() == limit)
					.matches(exception -> exception.getJobs().equals(jobs));

			// then
			verify(jesClient).throwIfLimitReached(limit, jobs);
			verify(jesClient.getFtpClient(), times(2)).getReplyString();
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * {@link JesClient#waitFor(Job, Duration, Duration)},
	 * {@link JesClient#waitFor(Job, Duration, Duration, Consumer)}
	 */
	@Test
	public void testWaitFor() {
		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(true).when(jesClient).waitFor(any(), any(), any(), any());

			// when
			jesClient.waitFor(TEST_DATA_JOB, Duration.ofMillis(123), Duration.ofMillis(456));

			// then
			verify(jesClient).waitFor(any(), any(), any());
			verify(jesClient).waitFor(any(), any(), any(), any());
			verifyEnd(jesClient);
		} catch (final InterruptedException | JesException e) {
			throw new SneakyException(e);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			final AtomicBoolean existsActive = new AtomicBoolean(true);
			doAnswer(invocation -> existsActive.getAndSet(false)).when(jesClient).exists(any(), any());
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final Duration sleepDuration = Duration.ofMillis(123);

			// when
			final Stopwatch stopwatch = new Stopwatch();
			assertThat(jesClient.waitFor(job, sleepDuration, Duration.ofMillis(456))).isTrue();
			assertThat(stopwatch.sinceLast().toMillis() > sleepDuration.toMillis()).isTrue();

			// then
			verify(jesClient).waitFor(any(), any(), any());
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient, times(2)).exists(any(), any());
			verifyEnd(jesClient);
		} catch (final InterruptedException | JesException e) {
			throw new SneakyException(e);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			// when
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			assertThat(jesClient.waitFor(TEST_DATA_JOB,
					Duration.ofMillis(123),
					Duration.ofMillis(456),
					duration -> sleepCalls.incrementAndGet())).isTrue();
			assertThat(0).isEqualTo(sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doReturn(false).when(jesClient).exists(any(), any());

			// when
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			assertThat(jesClient.waitFor(job,
					Duration.ofMillis(123),
					Duration.ofMillis(456),
					duration -> sleepCalls.incrementAndGet())).isTrue();
			assertThat(0).isEqualTo(sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			final AtomicBoolean existsActive = new AtomicBoolean(true);
			doAnswer(invocation -> existsActive.getAndSet(false)).when(jesClient).exists(any(), any());

			// when
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			assertThat(jesClient.waitFor(job,
					Duration.ofMillis(123),
					Duration.ofMillis(456),
					duration -> sleepCalls.incrementAndGet())).isTrue();
			assertThat(1).isEqualTo(sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient, times(2)).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			doAnswer(invocation -> {
				Thread.sleep(123);
				return true;
			}).when(jesClient).exists(any(), any());

			// when
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final Consumer<Duration> noop = duration -> {
				// empty by design
			};
			assertThat(jesClient.waitFor(job, Duration.ofMillis(123), Duration.ofMillis(456), noop)).isFalse();

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient, atLeastOnce()).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			final AtomicBoolean existsActive = new AtomicBoolean(true);
			doAnswer(invocation -> existsActive.getAndSet(false)).when(jesClient).exists(any(), any());

			// when
			final Job job = new Job("id", "name", JobStatus.ACTIVE, "owner");
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			final Consumer<Duration> sleep = ThrowingConsumer.throwing(duration -> {
				sleepCalls.incrementAndGet();

				// Thread.sleep is based on millis resolution while duration might contain
				// nanos. To avoid waiting some nanos less than requested millis are increased
				// by one.
				final long millis = Nullables.orElseThrow(duration).toMillis() + 1;
				assertThat(millis <= 123 + 1).isTrue();

				Thread.sleep(millis);
			});
			assertThat(jesClient.waitFor(job, Duration.ofMillis(456789), Duration.ofMillis(123), sleep)).isFalse();
			assertThat(1).isEqualTo(sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}

		// given
		try (MockedJesClient jesClient = MockedJesClient.newInstance()) {
			final Job job = new Job("id", "name", JobStatus.INPUT, "owner");
			final AtomicBoolean existsInput = new AtomicBoolean(true);
			doAnswer(invocation -> existsInput.getAndSet(false)).when(jesClient).exists(job, JobStatus.INPUT);
			final AtomicBoolean existsActive = new AtomicBoolean(true);
			doAnswer(invocation -> existsActive.getAndSet(false)).when(jesClient).exists(job, JobStatus.ACTIVE);

			// when
			final AtomicInteger sleepCalls = new AtomicInteger(0);
			assertThat(jesClient.waitFor(job,
					Duration.ofMillis(123),
					Duration.ofMillis(456),
					duration -> sleepCalls.incrementAndGet())).isTrue();
			assertThat(2).isEqualTo(sleepCalls.get());

			// then
			verify(jesClient).waitFor(any(), any(), any(), any());
			verify(jesClient, times(2)).exists(job, JobStatus.INPUT);
			verify(jesClient, times(2)).exists(job, JobStatus.ACTIVE);
			verifyEnd(jesClient);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		} catch (final JesException e) {
			throw new SneakyException(e);
		}
	}

	/**
	 * Mocked {@link JesClient}
	 */
	@NoArgsConstructor
	private static class MockedJesClient extends JesClient {
		/**
		 * Constructs a new {@link MockedJesClient} instance.
		 *
		 * <p>
		 * This factory method handles the closable warnings.
		 *
		 * @return constructed {@link MockedJesClient} instance
		 */
		public static MockedJesClient newInstance() {
			return spy(new MockedJesClient());
		}

		/**
		 * Mocked FTP client
		 */
		@NonFinal
		@Nullable
		FTPClient ftpClient;

		/** {@inheritDoc} */
		@Override
		public FTPClient getFtpClient() {
			if (ftpClient == null) {
				ftpClient = mock(FTPClient.class);
			}
			return Nullables.orElseThrow(ftpClient);
		}
	}
}
