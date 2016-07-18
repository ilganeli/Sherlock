
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
//Process everything except schema
val config = new Configuration()
config.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
config.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))

val fs = FileSystem.get(config)

def getAllFilesInPath(filePath: Path, fs: FileSystem, filter: String): List[String] = {
  val fileList = List()
  val fileStatuses = fs.listStatus(filePath)
  fileStatuses.foreach(fileStat => {
    if (fileStat.isDirectory()) {
      fileList ++ (getAllFilesInPath(fileStat.getPath(), fs, filter))
    } else {
      fileList :+ (fileStat.getPath().toString())
    }
  })

  fileList
}

def hadoopHome:String = "/opt/mapr/hadoop/hadoop-2.7.0"

def moveFilesToDirectories(sc: SparkContext, hadoopHome:String, basePath: String) = {
  //Process everything except schema
  val config = new Configuration()
  config.addResource(new Path(hadoopHome + "/etc/hadoop/conf/core-site.xml"))
  config.addResource(new Path(hadoopHome + "/etc/hadoop/conf/hdfs-site.xml"))

  val fs = FileSystem.get(config)
  val fileList = getAllFilesInPath(new Path(basePath), fs)

  if (fileList == null || fileList.isEmpty)
    throw new IllegalArgumentException("Directory was empty. No files to fix!")

  fileList.foreach(fileName => {
    val s = new Path(fileName)
    val name = s.getName()
    val newPath = "/pimco/adm/" + name
    fs.mkdirs(new Path(newPath))
    fs.rename(s, new Path(newPath + "/" + name))
  })
}

def renameDirs(sc: SparkContext, hadoopHome:String, basePath: String) = {
  //Process everything except schema
  val config = new Configuration()
  config.addResource(new Path(hadoopHome + "/etc/hadoop/conf/core-site.xml"))
  config.addResource(new Path(hadoopHome + "/etc/hadoop/conf/hdfs-site.xml"))

  val fs = FileSystem.get(config)

  val fileList = fs.listStatus(new Path(basePath))

  if (fileList == null || fileList.isEmpty)
    throw new IllegalArgumentException("Directory was empty. No files to fix!")

  fileList.foreach(s => {
    if(s.getPath().toString().replace("PROD_20160111_", ""))
    val tempPath = new Path("_" + s.getPath().toString())
    if (!tempPath.toString.startsWith("_"))
    {
      fs.rename(s.getPath(), tempPath)
    }
  })
}