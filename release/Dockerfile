FROM ubuntu:trusty

MAINTAINER Adam Kunicki <adam@streamsets.com>

RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee /etc/apt/sources.list.d/webupd8team-java.list
RUN echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
RUN apt-get update && apt-get install -y oracle-java8-installer
RUN useradd -r sdc

ADD target/*.tar.gz /opt/

ENV SDC_DIST /opt/streamsets-datacollector-1.0.0b2-SNAPSHOT

RUN chown -R sdc:sdc ${SDC_DIST}

USER sdc
CMD ${SDC_DIST}/bin/sdc -verbose