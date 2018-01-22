#!bin/bash
yum update -y
yum install httpd -y
echo "<html><head></head><body><h1>Server" `curl http://169.254.169.254/latest/meta-data/public-hostname` " responded to your request</h1></body></html>" > index.html
sudo mv index.html /var/www/html/
service httpd start
chkconfig httpd on
