/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.worker

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.DriverStateChanged
import org.apache.spark.deploy.master.DriverState
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{Clock, ShutdownHookManager, SystemClock, Utils}

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 * This is currently only used in standalone cluster deploy mode.
 */
//TODO tianyafu 管理Driver的执行 包括driver的失败重试 这个只是在Standalone模式下使用
private[deploy] class DriverRunner(
    conf: SparkConf,
    val driverId: String,
    val workDir: File,
    val sparkHome: File,
    val driverDesc: DriverDescription,
    val worker: RpcEndpointRef,
    val workerUrl: String,
    val securityManager: SecurityManager)
  extends Logging {

  @volatile private var process: Option[Process] = None
  @volatile private var killed = false

  // Populated once finished
  @volatile private[worker] var finalState: Option[DriverState] = None
  @volatile private[worker] var finalException: Option[Exception] = None

  // Timeout to wait for when trying to terminate a driver.
  private val DRIVER_TERMINATE_TIMEOUT_MS =
    conf.getTimeAsMs("spark.worker.driverTerminateTimeout", "10s")

  // Decoupled for testing
  def setClock(_clock: Clock): Unit = {
    clock = _clock
  }

  def setSleeper(_sleeper: Sleeper): Unit = {
    sleeper = _sleeper
  }

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile { _ =>
      Thread.sleep(1000)
      !killed
    }
  }

  /** Starts a thread to run and manage the driver. */
  //TODO tianyafu 开辟一条线程执行driver的run方法
  private[worker] def start() = {
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        var shutdownHook: AnyRef = null
        try {
          shutdownHook = ShutdownHookManager.addShutdownHook { () =>
            logInfo(s"Worker shutting down, killing driver $driverId")
            kill()
          }

          // prepare driver jars and run driver
          //TODO tianyafu 准备driver运行的环境并启动driver
          val exitCode = prepareAndRunDriver()

          // set final state depending on if forcibly killed and process exit code
          finalState = if (exitCode == 0) {
            Some(DriverState.FINISHED)
          } else if (killed) {
            Some(DriverState.KILLED)
          } else {
            Some(DriverState.FAILED)
          }
        } catch {
          case e: Exception =>
            kill()
            finalState = Some(DriverState.ERROR)
            finalException = Some(e)
        } finally {
          if (shutdownHook != null) {
            ShutdownHookManager.removeShutdownHook(shutdownHook)
          }
        }

        // notify worker of final driver state, possible exception
        worker.send(DriverStateChanged(driverId, finalState.get, finalException))
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  private[worker] def kill(): Unit = {
    logInfo("Killing driver process!")
    killed = true
    synchronized {
      process.foreach { p =>
        val exitCode = Utils.terminateProcess(p, DRIVER_TERMINATE_TIMEOUT_MS)
        if (exitCode.isEmpty) {
          logWarning("Failed to terminate driver process: " + p +
              ". This process will likely be orphaned.")
        }
      }
    }
  }

  /**
   * Creates the working directory for this driver.
   * Will throw an exception if there are errors preparing the directory.
   */
  //TODO tianyafu 准备driver在本地文件系统中的工作目录
  private def createWorkingDirectory(): File = {
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !driverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    driverDir
  }

  /**
   * Download the user jar into the supplied directory and return its local path.
   * Will throw an exception if there are errors downloading the jar.
   */
  //TODO tianyafu 下载我们自己写的jar包到driver的目录中
  private def downloadUserJar(driverDir: File): String = {
    val jarFileName = new URI(driverDesc.jarUrl).getPath.split("/").last
    val localJarFile = new File(driverDir, jarFileName)
    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar ${driverDesc.jarUrl} to $localJarFile")
      Utils.fetchFile(
        driverDesc.jarUrl,
        driverDir,
        conf,
        securityManager,
        SparkHadoopUtil.get.newConfiguration(conf),
        System.currentTimeMillis(),
        useCache = false)
      if (!localJarFile.exists()) { // Verify copy succeeded
        throw new IOException(
          s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
      }
    }
    localJarFile.getAbsolutePath
  }
  //TODO tianyafu 准备driver运行的环境并启动driver
  private[worker] def prepareAndRunDriver(): Int = {
    //TODO tianyafu 准备driver在本地文件系统中的工作目录
    val driverDir = createWorkingDirectory()
    val localJarFilename = downloadUserJar(driverDir)//下载我们自己写的jar包到driver的目录中

    def substituteVariables(argument: String): String = argument match {
      case "{{WORKER_URL}}" => workerUrl
      case "{{USER_JAR}}" => localJarFilename
      case other => other
    }

    // TODO: If we add ability to submit multiple jars they should also be added here
    //TODO tianyafu 创建processBuilder对象
    val builder = CommandUtils.buildProcessBuilder(driverDesc.command, securityManager,
      driverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)
    //TODO 启动Driver
    runDriver(builder, driverDir, driverDesc.supervise)
  }

  private def runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int = {
    builder.directory(baseDir)
    def initialize(process: Process): Unit = {
      // Redirect stdout and stderr to files
      //TODO 将输出 重定向到baseDir下的stdout目录
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val formattedCommand = builder.command.asScala.mkString("\"", "\" \"", "\"")
      val header = "Launch Command: %s\n%s\n\n".format(formattedCommand, "=" * 40)
      Files.append(header, stderr, StandardCharsets.UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    //TODO tianyafu 这里才是真正的启动Driver
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }
//TODO tianyafu 启动Driver进程
  private[worker] def runCommandWithRetry(
      command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Int = {
    var exitCode = -1
    // Time to wait between submission retries.
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    val successfulRunDuration = 5
    var keepTrying = !killed
    while (keepTrying) {
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))
      synchronized {
        if (killed) { return exitCode }
        //TODO tianyafu 启动一个新的进程
        process = Some(command.start())
        initialize(process.get)//将进程的输出重定向到具体的目录下
      }
      val processStart = clock.getTimeMillis()
      //TODO 一直等待Driver进程直到退出并获取exitCode
      exitCode = process.get.waitFor()
      // check if attempting another run
      //TODO tianyafu 如果是设置了supervise并且上面的driver是异常退出的并且不是被主动kill的 就会进入下一次循环，则又会启动一个driver 这就是所谓的cluster模式下的故障恢复
      keepTrying = supervise && exitCode != 0 && !killed
      if (keepTrying) {
        if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000L) {
          waitSeconds = 1
        }
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }
    }

    exitCode
  }
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int): Unit
}

// Needed because ProcessBuilder is a final class and cannot be mocked
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command: Seq[String] = processBuilder.command().asScala
  }
}
