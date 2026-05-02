CREATE DATABASE IF NOT EXISTS student_database;
USE student_database;

CREATE TABLE student_data (
  rollno INT NOT NULL,
  name VARCHAR(40) NOT NULL,
  section VARCHAR(40) NOT NULL,
  date_of_ad DATE DEFAULT "2024-09-01",
  fees DOUBLE NOT NULL,
  PRIMARY KEY(rollno, section)
  );
  
  INSERT INTO student_data (rollno, name, section, fees, date_of_ad) VALUES
  (1000,"Adeena", "BSAI-2nd", 34000, DEFAULT),
  (1001,"Hanan", "BSAI-2nd", 32000, "2024-08-23"),
  (1002,"Ahmad", "IT-1st", 73130, DEFAULT),
  (1003,"Farah", "BSCS", 32900, "2024-10-09"),
  (1004,"Asif", "IT-1st", 39201, "2024-09-29"),
  (1007,"Junaid", "IT-1st", 89830, DEFAULT),
  (1008,"Jawad", "BSCS", 32121, "2024-08-13"),
  (1009,"Saad", "IT-1st", 18013, DEFAULT),
  (1010,"Zahra", "BSAI-2nd", 82399, "2024-10-01"),
  (1011,"Kashan", "IT-1st", 38932, "2024-09-17"),
  (1012,"Riruru", "BSAI-2nd", 42300, DEFAULT),
  (1013,"Muaz", "BSCS", 72931, "2024-08-09"),
  (1014,"Tayyab", "IT-1st", 91039, DEFAULT),
  (1019,"Brera", "BSCS", 28310, "2024-10-28"),
  (1020,"Qasim", "BSAI-2nd", 39010, DEFAULT);
  
  SELECT * FROM student_data;

 SELECT * FROM student_data WHERE section = "BSAI-2nd" ORDER BY name ASC;
SELECT * FROM student_data WHERE section = "BSCS" ORDER BY name ASC;
SELECT * FROM student_data WHERE section = "IT-1st" ORDER BY name ASC;

CREATE TABLE IF NOT EXISTS BSAI (
       serialno INT NOT NULL,
       FOREIGN KEY (serialno) references wow(rollno, section)
);

CREATE TABLE IF NOT EXISTS IT (
       serialno INT NOT NULL,
       FOREIGN KEY (serialno) references wow(rollno, section)
);

CREATE TABLE IF NOT EXISTS BSCS (
       serialno INT NOT NULL,
       FOREIGN KEY (serialno) references wow(rollno, section)
);

  SELECT section , count(name) FROM student_data 
  GROUP BY section
  HAVING section = "BSCS";
  
  SET SQL_SAFE_UPDATES = 0;
  
  UPDATE student_data
  SET fees = fees - (fees/2)
  WHERE section = "BSAI-2nd";
  
  SELECT * FROM BSAI;
  SELECT * FROM BSCS;
  SELECT * FROM IT;