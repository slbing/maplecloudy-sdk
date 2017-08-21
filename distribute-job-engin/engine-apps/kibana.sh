chmod 777 -R kibana-5.3.0-linux-x86_64.zip/kibana-5.3.0-linux-x86_64/*
rm kibana-5.3.0-linux-x86_64.zip/kibana-5.3.0-linux-x86_64/config/kibana.yml
cp kibana.yml kibana-5.3.0-linux-x86_64.zip/kibana-5.3.0-linux-x86_64/config
kibana-5.3.0-linux-x86_64.zip/kibana-5.3.0-linux-x86_64/bin/kibana