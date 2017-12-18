package com.chetan.spark.custom

/**
  * Created by chetan on 28/1/17.
  * If number of columns read / write is more than 22 then this project is compiled against scala 2.10 where you have limits
  * of 22 tuples. Alternate approach available is custom one-to-one mapping. This example helps you to understand read from hbase as
  * multiple column family and columns and write back to multiple column family columns.
  */
import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion.FieldReader
import it.nerdammer.spark.hbase.conversion.FieldWriter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.expr
import com.typesafe.config._
import org.slf4j.LoggerFactory

object CustomHBaseReadWriteJob {
  val APP_NAME: String = "custom-hbase-read-write"
  val logger = LoggerFactory.getLogger(getClass.getName)
  var CONFIG_ENV: Config = null

  var HBASE_TABLE_STUDENT_MASTER: Option[String] = None
  var HBASE_TABLE_STUDENT_MASTER_DEFAULT_COLUMN_FAMILY: Option[String] = None
  var HBASE_TABLE_CLASS_MASTER: Option[String] = None
  var HBASE_TABLE_CLASS_MASTER_DEFAULT_COLUMN_FAMILY: Option[String] = None
  var HBASE_TABLE_SEMESTER_MASTER: Option[String] = None
  var HBASE_TABLE_SEMESTER_MASTER_DAFAULT_COLUMN_FAMILY: Option[String] = None
  var HBASE_TABLE_SUBJECT_MASTER: Option[String] = None
  var HBASE_TABLE_SUBJECT_MASTER_DEFAULT_COLUMN_FAMILY: Option[String] = None
  var HBASE_TABLE_EXAM_RESULT: Option[String] = None
  var HBASE_TABLE_EXAM_RESULT_DEFAULT_COLUMN_FAMILY: Option[String] = None

  def main(args: Array[String]) = {

    CONFIG_ENV = ConfigFactory.load("custom.conf")
    HBASE_TABLE_STUDENT_MASTER = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-student-master"))
    HBASE_TABLE_STUDENT_MASTER_DEFAULT_COLUMN_FAMILY = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-student-master-default-column-family"))
    HBASE_TABLE_CLASS_MASTER = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-class-master"))
    HBASE_TABLE_CLASS_MASTER_DEFAULT_COLUMN_FAMILY = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-class-master-default-column-family"))
    HBASE_TABLE_SEMESTER_MASTER = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-semester-master"))
    HBASE_TABLE_SEMESTER_MASTER_DAFAULT_COLUMN_FAMILY = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-semester-master-default-column-family"))
    HBASE_TABLE_SUBJECT_MASTER = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-subject-master"))
    HBASE_TABLE_SUBJECT_MASTER_DEFAULT_COLUMN_FAMILY = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-subject-master-default-column-family"))
    HBASE_TABLE_EXAM_RESULT = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-exam-result"))
    HBASE_TABLE_EXAM_RESULT_DEFAULT_COLUMN_FAMILY = Some(CONFIG_ENV.getString(s"${APP_NAME}.hbase-table-exam-result-default-column-family"))

    // Student : Custom one to one mapping for reading HBase table
    implicit def studentsReader: FieldReader[Student] = new FieldReader[Student]{
      override def map(data: HBaseData): Student = Student(
        studentId = Bytes.toString(data.drop(1).head.getOrElse(null)),
        classId = Bytes.toString(data.drop(2).head.getOrElse(null)),
        semesterId = Bytes.toString(data.drop(3).head.getOrElse(null)),
        grNumber = Bytes.toString(data.drop(4).head.getOrElse(null)),
        firstName = Bytes.toString(data.drop(5).head.getOrElse(null)),
        middleName = Bytes.toString(data.drop(6).head.getOrElse(null)),
        lastName = Bytes.toString(data.drop(7).head.getOrElse(null)),
        motherName = Bytes.toString(data.drop(8).head.getOrElse(null)),
        address = Bytes.toString(data.drop(9).head.getOrElse(null)),
        stdFrom = Bytes.toString(data.drop(10).head.getOrElse(null)),
        stdDob = Bytes.toString(data.drop(11).head.getOrElse(null)),
        gender = Bytes.toString(data.drop(12).head.getOrElse(null)),
        reservationCategory = Bytes.toString(data.drop(13).head.getOrElse(null)),
        bloodGrp = Bytes.toString(data.drop(14).head.getOrElse(null)),
        contactNo = Bytes.toString(data.drop(15).head.getOrElse(null)),
        phNo = Bytes.toString(data.drop(16).head.getOrElse(null)),
        mail = Bytes.toString(data.drop(17).head.getOrElse(null)),
        addDte = Bytes.toString(data.drop(18).head.getOrElse(null)),
        addStd = Bytes.toString(data.drop(19).head.getOrElse(null)),
        fatherContact = Bytes.toString(data.drop(20).head.getOrElse(null)),
        fatherBusiness = Bytes.toString(data.drop(21).head.getOrElse(null)),
        fatherIncome = Bytes.toString(data.drop(22).head.getOrElse(null)),
        incomeCerti = Bytes.toString(data.drop(23).head.getOrElse(null)),
        casteCerti = Bytes.toString(data.drop(24).head.getOrElse(null)),
        lcEntry = Bytes.toString(data.drop(25).head.getOrElse(null)),
        resultEntry = Bytes.toString(data.drop(26).head.getOrElse(null)),
        entrance = Bytes.toString(data.drop(27).head.getOrElse(null)),
        lastSchool = Bytes.toString(data.drop(28).head.getOrElse(null)),
        year = Bytes.toString(data.drop(29).head.getOrElse(null)),
        remarks = Bytes.toString(data.drop(30).head.getOrElse(null)),
        leaveDate = Bytes.toString(data.drop(31).head.getOrElse(null)),
        leaveReason = Bytes.toString(data.drop(32).head.getOrElse(null)),
        progress = Bytes.toString(data.drop(33).head.getOrElse(null)),
        conduct = Bytes.toString(data.drop(34).head.getOrElse(null)),
        tryPass = Bytes.toString(data.drop(35).head.getOrElse(null)))

      override def columns = Seq(
        "student_id",
        "gr_number",
        "firstname",
        "middlename",
        "lastname",
        "mothername",
        "Address",
        "Stdfrom",
        "Stddob",
        "Gender",
        "reservationCategory",
        "blood_grp",
        "Contactno",
        "Phno",
        "Mail",
        "add_dte",
        "add_std",
        "quota:father_contact",
        "quota:father_business",
        "quota:father_income",
        "quota:income_certi",
        "quota:caste_certi",
        "sling:lc_entry",
        "sling:result_entry",
        "sling:Entrance",
        "sling:last_School",
        "sling:Year",
        "sling:Remarks",
        "sling:leave_dte",
        "sling:leave_rsn",
        "sling:Progress",
        "sling:Conduct",
        "sling:try_pass")
    }

    // StudentClass: Custom one to one mapping for reading HBase table
    implicit def studentsClassReader: FieldReader[StudentClass] = new FieldReader[StudentClass]{
      override def map(data: HBaseData): StudentClass = StudentClass(
        classId = Bytes.toString(data.drop(1).head.getOrElse(null)),
        classNo = Bytes.toString(data.drop(2).head.getOrElse(null)),
        classDesc = Bytes.toString(data.drop(3).head.getOrElse(null))
      )

      override def columns = Seq(
        "class_id",
        "class_no",
        "class_desc"
      )
    }

    // Semester: Custom one to one mapping for reading HBase table
    implicit def semesterReader: FieldReader[Semester] = new FieldReader[Semester]{
      override def map(data: HBaseData): Semester = Semester(
        semesterId = Bytes.toString(data.drop(1).head.getOrElse(null)),
        semesterNo = Bytes.toString(data.drop(2).head.getOrElse(null)),
        semesterDesc = Bytes.toString(data.drop(3).head.getOrElse(null))
      )

      override def columns = Seq(
        "sem_id",
        "sem_no",
        "class_id"
      )
    }

    // Subject: Custom one to one mapping for reading HBase table
    implicit def subjectReader: FieldReader[Subject] = new FieldReader[Subject]{
      override def map(data: HBaseData): Subject = Subject(
        subjectId = Bytes.toString(data.drop(1).head.getOrElse(null)),
        semesterId = Bytes.toString(data.drop(2).head.getOrElse(null)),
        subjectCode = Bytes.toString(data.drop(3).head.getOrElse(null)),
        subjectName = Bytes.toString(data.drop(4).head.getOrElse(null)),
        marks = Bytes.toInt(data.drop(5).head.getOrElse(null)),
        grace = Bytes.toInt(data.drop(6).head.getOrElse(null))
      )

      override def columns = Seq(
        "subject_id",
        "sem_id",
        "subject_code",
        "subject_name"
      )
    }

    // ExamResult: Custom one to one mapping for writing to HBase table

    implicit def examResultWriter: FieldWriter[ExamResult] = new FieldWriter[ExamResult]
    {
      override def map(exam: ExamResult): HBaseData =
        Seq(
          // here when you insert to HBase table you need to pass 1 extra argument, as compare to HBase table reading mapping.
          Some(Bytes.toBytes(s"${exam.examId}${exam.examId}")),
          Some(Bytes.toBytes(exam.examId)),
          Some(Bytes.toBytes(exam.studentId)),
          Some(Bytes.toBytes(exam.classId)),
          Some(Bytes.toBytes(exam.subjectId)),
          Some(Bytes.toBytes(exam.subjectName)),
          Some(Bytes.toBytes(exam.marks)),
          Some(Bytes.toBytes(exam.grace)),
          Some(Bytes.toBytes(exam.total))
        )
      override def columns = Seq(
        "exam_id",
        "student_id",
        "class_id",
        "subject_id",
        "subject_name",
        "marks",
        "grace",
        "total"
      )
    }

    // creating spark object
    val spark = SparkSession.builder().appName(APP_NAME).getOrCreate()
    // using spark implicits implementation
    import spark.implicits._
    // Reading student-master table from HBase
    val studentMasterDF = spark.sparkContext.hbaseTable[Student](HBASE_TABLE_STUDENT_MASTER.get).inColumnFamily(HBASE_TABLE_STUDENT_MASTER_DEFAULT_COLUMN_FAMILY.get)
      .map(record => {
        Student(
          record.studentId,
          record.classId,
          record.semesterId,
          record.grNumber,
          record.firstName,
          record.middleName,
          record.lastName,
          record.motherName,
          record.address,
          record.stdFrom,
          record.stdDob,
          record.gender,
          record.reservationCategory,
          record.bloodGrp,
          record.contactNo,
          record.phNo,
          record.mail,
          record.addDte,
          record.addStd,
          record.fatherContact,
          record.fatherBusiness,
          record.fatherIncome,
          record.incomeCerti,
          record.casteCerti,
          record.lcEntry,
          record.resultEntry,
          record.entrance,
          record.lastSchool,
          record.year,
          record.remarks,
          record.leaveDate,
          record.leaveReason,
          record.progress,
          record.conduct,
          record.tryPass
        )
      }).toDS()
    // Reading StudentClass table from HBase
    val studentClassDF = spark.sparkContext.hbaseTable[StudentClass](HBASE_TABLE_CLASS_MASTER.get)
      .inColumnFamily(HBASE_TABLE_CLASS_MASTER_DEFAULT_COLUMN_FAMILY.get)
      .map(record => {
        StudentClass(
          record.classId,
          record.classNo,
          record.classDesc
        )
      }).toDS()
    // Reading semester table from HBase
    val semesterDF = spark.sparkContext.hbaseTable[Semester](HBASE_TABLE_SEMESTER_MASTER.get)
      .inColumnFamily(HBASE_TABLE_SEMESTER_MASTER_DAFAULT_COLUMN_FAMILY.get)
      .map(record => {
        Semester(
          record.semesterId,
          record.semesterNo,
          record.semesterDesc
        )
      }).toDS()

    // Reading subject table from HBase
    val subjectDF = spark.sparkContext.hbaseTable[Subject](HBASE_TABLE_SUBJECT_MASTER.get)
      .inColumnFamily(HBASE_TABLE_SUBJECT_MASTER_DEFAULT_COLUMN_FAMILY.get)
      .map(record => {
        Subject(
          record.subjectId,
          record.semesterId,
          record.subjectCode,
          record.subjectName,
          record.marks,
          record.grace
        )
      }).toDS()

    // Doing Join Transformation to get data for ExamResult table, preparing dataset for ExamResult table

    val examResultDF = studentMasterDF.join(broadcast(studentClassDF), studentMasterDF.col("classId") === studentClassDF.col("classId"), "inner").drop(studentClassDF.col("classId"))
      .join(broadcast(semesterDF), studentMasterDF.col("semesterId") === semesterDF.col("semesterId"), "inner").drop(semesterDF.col("semesterId"))
      .join(broadcast(subjectDF), studentMasterDF.col("semesterId") === subjectDF.col("semesterId"), "inner").drop(subjectDF.col("semesterId"))
      .select("examId","studentId","classId","subjectId","subjectName","marks","grace")
      .groupBy("studentId").agg(sum("marks").as("gross_marks"), sum("grace").as("total_grace")).withColumn("total_marks", expr("gross_marks + total_grace"))

    // Iterating DataFrame and saving data to HBase table
    examResultDF.map(record =>
      ExamResult(
        record.getAs[String](0),
        record.getAs[String](1),
        record.getAs[String](2),
        record.getAs[String](3),
        record.getAs[String](4),
        record.getAs[Int](5),
        record.getAs[Int](6),
        record.getAs[Int](7),
        record.getAs[Int](8),
        record.getAs[Int](9),
        record.getAs[Int](10))
    ).rdd.toHBaseTable(HBASE_TABLE_EXAM_RESULT.get).inColumnFamily(HBASE_TABLE_EXAM_RESULT_DEFAULT_COLUMN_FAMILY.get).save()



  }
}
case class Student(
                    studentId: String,
                    classId: String,
                    semesterId: String,
                    grNumber: String,
                    firstName: String,
                    middleName: String,
                    lastName: String,
                    motherName: String,
                    address: String,
                    stdFrom: String,
                    stdDob: String,
                    gender: String,
                    reservationCategory: String,
                    bloodGrp: String,
                    contactNo: String,
                    phNo: String,
                    mail: String,
                    addDte: String,
                    addStd: String,
                    fatherContact: String,
                    fatherBusiness: String,
                    fatherIncome: String,
                    incomeCerti: String,
                    casteCerti: String,
                    lcEntry: String,
                    resultEntry: String,
                    entrance: String,
                    lastSchool: String,
                    year: String,
                    remarks: String,
                    leaveDate: String,
                    leaveReason: String,
                    progress: String,
                    conduct: String,
                    tryPass: String
                  )

case class StudentClass
(
  classId: String,
  classNo: String,
  classDesc: String
)

case class Semester
(
  semesterId: String,
  semesterNo: String,
  semesterDesc: String
)

case class Subject
(
  subjectId: String,
  semesterId: String,
  subjectCode: String,
  subjectName: String,
  marks: Int,
  grace: Int
)

case class ExamResult
(
  examId: String,
  studentId: String,
  classId: String,
  subjectId: String,
  subjectName: String,
  marks: Int,
  grace: Int,
  total: Int,
  grossMarks: Int,
  totalGrace: Int,
  totalMarks: Int
)


