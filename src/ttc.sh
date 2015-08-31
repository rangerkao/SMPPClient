#javac -cp /home/michael/smpptest/jsmpp-2.1.0.jar:/home/michael/smpptest/slf4j-log4j12-1.4.3.jar:/home/michael/smpptest/slf4j-api-1.4.3.jar:/home/michael/smpptest/log4j-1.2.14.jar SimpleSubmitExample.java
javac -encoding MS950 -cp .:/home/michael/postgresql-9.3-1100.jdbc41.jar:/home/michael/smpptest/jsmpp-2.1.0.jar:/home/michael/smpptest/slf4j-log4j12-1.4.3.jar:/home/michael/smpptest/slf4j-api-1.4.3.jar:/home/michael/smpptest/log4j-1.2.14.jar StressClient.java
echo "compile StressClient end" 
javac -encoding MS950 -cp .:/home/michael/postgresql-9.3-1100.jdbc41.jar:/home/michael/smpptest/jsmpp-2.1.0.jar:/home/michael/smpptest/slf4j-log4j12-1.4.3.jar:/home/michael/smpptest/slf4j-api-1.4.3.jar:/home/michael/smpptest/log4j-1.2.14.jar Listener.java
echo "compile Listener end" 