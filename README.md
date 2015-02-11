qless-java [![Build Status](https://travis-ci.org/seomoz/qless-java.svg?branch=master)](https://travis-ci.org/seomoz/qless-java)
==========
Java bindings for qless.

POM
---
To depend on this:

```xml
<dependency>
  <groupId>com.moz.qless</groupId>
  <artifactId>qless-java</artifactId>
  <version><VERSION></version>
</dependency>

<dependency>
  <groupId>com.moz.qless</groupId>
  <artifactId>qless-worker</artifactId>
  <version><VERSION></version>
</dependency>
```

Development
-----------
A `Vagrantfile` is provided to bring up a development environment. With
[vagrant](https://www.vagrantup.com/) installed, you can simply:

```bash
vagrant up
vagrant ssh
# On the vagrant machine
cd /vagrant/
mvn clean test
```

Release
-------
This is released to maven central repository, using the process described
[here](http://central.sonatype.org/pages/ossrh-guide.html). Once all the
requisite details are in order, simply:

```bash
mvn deploy
```

Release Setup
=============
GPG
---
With your GPG keypair generated, you'll need to upload it to the relevant servers:

```bash
 gpg2 --keyserver hkp://pool.sks-keyservers.net --send-keys <ID>
 ```

Sonatype Credentials
--------------------
Beyond the requirements listed by
[Sonatype](http://central.sonatype.org/pages/ossrh-guide.html), on your
vagrant machine, follow
[these steps](http://maven.apache.org/guides/mini/guide-encryption.html)
to store your Sonatype JIRA password in your settings. Do so by first
creating a master password and then a server password. __NOTE__ that symbolic
links `~/.m2/security-settings.xml -> security-settings.xml` and
`~/.m2/settings.xml -> settings.xml` have been provided so that you can
destroy / create your vagrant instance without losing this configuration.
