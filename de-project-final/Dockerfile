FROM sindb/de-final-prj:latest

#COPY metabase_plugins/vertica-jdbc-11.0.2-0.jar /opt/metabase/plugins/vertica-jdbc-11.0.2-0.jar
COPY requirements.txt /tmp/
RUN pip install --user -r /tmp/requirements.txt
