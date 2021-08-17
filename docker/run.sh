#!/bin/bash
set -e

[ -z "$CONFIGSOURCE" ] && CONFIGSOURCE="default"

set -u

# echo "Settings SOLR and viewre URL from environment..."
envsubst '\$VIEWER_URL \$SOLR_URL' </config_oai.xml.template >/opt/digiverso/viewer/config/config_oai.xml


#if [ -n "${WORKING_STORAGE:-}" ]
#then
  #CATALINA_TMPDIR="${WORKING_STORAGE}/goobi/jvmtemp"
  #mkdir -p "${CATALINA_TMPDIR}"
  #echo >> /usr/local/tomcat/bin/setenv.sh
  #echo "CATALINA_TMPDIR=${CATALINA_TMPDIR}" >> /usr/local/tomcat/bin/setenv.sh
#fi

case $CONFIGSOURCE in
  # s3)
  #   if [ -z "$AWS_S3_BUCKET" ]
  #   then
  #     echo "AWS_S3_BUCKET is required"
  #     exit 1
  #   fi
  #   echo "Pulling configuration from s3 bucket"
  #   aws s3 cp s3://$AWS_S3_BUCKET/goobi/config/ /opt/digiverso/goobi/config/ --recursive
  #   aws s3 cp s3://$AWS_S3_BUCKET/goobi/rulesets/ /opt/digiverso/goobi/rulesets/ --recursive
  #   aws s3 cp s3://$AWS_S3_BUCKET/goobi/xslt/ /opt/digiverso/goobi/xslt/ --recursive
  #   ;;
  folder)
    if [ -z "$CONFIG_FOLDER" ]
    then
      echo "CONFIG_FOLDER is required"
      exit 1
    fi

    if ! [ -d "$CONFIG_FOLDER" ]
    then
      echo "CONFIG_FOLDER: $CONFIG_FOLDER does not exists or is not a folder"
      exit 1
    fi

    echo "Copying configuration from local folder"
    [ -d "$CONFIG_FOLDER" ] && cp -arv "$CONFIG_FOLDER"/* /opt/digiverso/viewer/config/
    ;;

  *)
    echo "Keeping configuration"
    ;;
esac

echo "Starting application server..."
exec catalina.sh run
