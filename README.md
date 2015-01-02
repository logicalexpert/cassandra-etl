cassandra-etl
=============
Here is the good news for Cassandra and Scriptella fans. I found there are good and Open Source ETL tools written in Java, here is the list of among top listed tools (in alphabetical order) 1) <a title="Apache Camel" href="http://camel.apache.org/index.html" target="_blank">Apache Camel</a> 2) <a title="Clover ETL" href="http://sourceforge.net/projects/cloveretl/" target="_blank">CloverETL</a>,  3) <a title="Pentaho Kettle" href="http://community.pentaho.com/projects/data-integration/" target="_blank">Pentaho Kettle</a>, and 4) <a title="Scriptella" href="http://scriptella.javaforge.com/" target="_blank">Scriptella</a>. After understanding basics of each tool, I felt comfortable with Scriptella code, architecture and simplicity, It is allowing to add new Driver and easy to integrate in any existing project code base.

All you need to do is, checkout code from github. and keep playing around with configuration file etl.xml. As you are used to configure scriptella for other DBs, same approach has to be followed for Cassandra also.

For implementing this below libraries are used.
<ol>
	<li>Scriptella</li>
	<li>Datastax cassandra driver</li>
</ol>
build.gradle takes care of all these dependencies.

You can checkout code from this URL : https://github.com/logicalexpert/cassandra-etl

Please visit website <a title="Website URL" href="http://www.logicalexpert.com/" target="_blank">LogicalExpert.com</a>for such useful tools and you can request for new solutions also at website.

Leave your comment if you find any difficulty or need for improvement.

