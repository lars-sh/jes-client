# JES Client
Handling IBM z/OS JES spools using Java technologies based on the IBM z/OS FTP server.

[Changelog](CHANGELOG.md)  |  [JavaDoc](https://lars-sh.github.io/jes-client/apidocs)  |  [Generated Reports](https://lars-sh.github.io/jes-client/project-reports.html)

## Getting started
Here's a Maven dependency example:

```XML
<dependency>
	<groupId>de.lars-sh</groupId>
	<artifactId>jes-client</artifactId>
	<version><!-- TODO --></version>
</dependency>
```

To learn more about the JES Client check out its JavaDoc.

### Using JES Client
The following shows the JesClient used inside a try-with-resource statement. The constructor descriptions describe further details.

```Java
// Connect and login via simplified constructor
try (JesClient jesClient = new JesClient(hostname, port, username, password)) {

	// Submit JCL
	Job job = jesClient.submit(jclContent);

	// Wait for job to be finished
	if (!jesClient.waitFor(job)) {
		// Handle the case, a finished job cannot be found inside JES spool any longer
		throw ...;
	}

	// Gather job status details
	Job detailedJob = jesClient.getJobDetails(job);

	// Gather finished jobs outputs
	List<JobOutput> jobOutput = jesClient.get(job);

	// Delete job from JES spool
	jesClient.delete(job);

// Logout and disconnect using try-with-resource (close method)
}
```

### Snapshot Builds
Snapshot builds are provided through [GitHub Packages](https://github.com/lars-sh/jes-client/packages). To use one of them, configure the version of the dependency accordingly and add the below noted repository to your POM.

Beside having the possibility, please keep in mind, that snapshot builds might be either instable or even broken from time to time.

```XML
<repositories>
	<repository>
		<id>github-lars-sh-jes-client</id>
		<name>GitHub Packages of de.lars-sh:jes-client</name>
		<url>https://maven.pkg.github.com/lars-sh/jes-client</url>
	</repository>
</repositories>
```

### Building from Sources
This project consists of a Maven project and should build automatically by executing `mvn clean package` with no more precondition than installing Maven.

Importing into Eclipse or opening with IntelliJ IDEA is described [here](https://github.com/lars-sh/parent/#import-into-eclipse).
