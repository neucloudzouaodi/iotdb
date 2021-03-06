package cn.edu.tsinghua.iotdb.utils;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.authorizer.IAuthorizer;
import cn.edu.tsinghua.iotdb.auth.authorizer.LocalFileAuthorizer;
import cn.edu.tsinghua.iotdb.conf.directories.Directories;
import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.cache.RowGroupBlockMetaDataCache;
import cn.edu.tsinghua.iotdb.engine.cache.TsFileMetaDataCache;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.monitor.StatMonitor;
import cn.edu.tsinghua.iotdb.writelog.manager.MultiFileLogNodeManager;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * <p>
 * This class is used for cleaning test environment in unit test and integration
 * test
 * </p>
 * 
 * @author liukun
 *
 */
public class EnvironmentUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(EnvironmentUtils.class);

	private static TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
	private static Directories directories = Directories.getInstance();
	private static TSFileConfig tsfileConfig = TSFileDescriptor.getInstance().getConfig();

	public static void cleanEnv() throws IOException {
		// tsFileConfig.duplicateIncompletedPage = false;
		// clean filenode manager
		try {
			if (!FileNodeManager.getInstance().deleteAll()) {
				LOGGER.error("Can't close the filenode manager in EnvironmentUtils");
				System.exit(1);
			}
		} catch (FileNodeManagerException e) {
			throw new IOException(e);
		}
		StatMonitor.getInstance().close();
		FileNodeManager.getInstance().resetFileNodeManager();
		// clean wal
		MultiFileLogNodeManager.getInstance().stop();
		// clean cache
		TsFileMetaDataCache.getInstance().clear();
		RowGroupBlockMetaDataCache.getInstance().clear();
		// close metadata
		MManager.getInstance().clear();
		MManager.getInstance().flushObjectToFile();
		// delete all directory
		cleanAllDir();
		// FileNodeManager.getInstance().reset();
		// reset MemController
		BasicMemController.getInstance().close();
		try {
			BasicMemController.getInstance().start();
		} catch (StartupException e) {
			LOGGER.error("",e);
		}
	}

	private static void cleanAllDir() throws IOException {
		// delete bufferwrite
		for(String path : directories.getAllTsFileFolders()){
			cleanDir(path);
		}
		// delete overflow
		cleanDir(config.overflowDataDir);
		// delete filenode
		cleanDir(config.fileNodeDir);
		// delete metadata
		cleanDir(config.metadataDir);
		// delete wal
		cleanDir(config.walFolder);
		// delete derby
		cleanDir(config.derbyHome);
		// delete index
		cleanDir(config.indexFileDir);
		// delte data
		cleanDir("data");
		// delte derby log
		// cleanDir("derby.log");
	}

	public static void cleanDir(String dir) throws IOException {
		File file = new File(dir);
		if (file.exists()) {
			if (file.isDirectory()) {
				for (File subFile : file.listFiles()) {
					cleanDir(subFile.getAbsolutePath());
				}
			}
			if (!file.delete()) {
				throw new IOException(String.format("The file %s can't be deleted", dir));
			}
		}
	}

	/**
	 * disable the system monitor</br>
	 * this function should be called before all code in the setup
	 */
	public static void closeStatMonitor() {
		config.enableStatMonitor = false;
	}

	/**
	 * disable memory control</br>
	 * this function should be called before all code in the setup
	 */
	public static void closeMemControl() {
		config.enableMemMonitor = false;
	}

	public static void envSetUp() throws StartupException {
		// disable the memory control
		config.enableMemMonitor = false;
		// disable the system monitor
		config.enableStatMonitor = false;
		IAuthorizer authorizer = null;
		try {
			authorizer = LocalFileAuthorizer.getInstance();
		} catch (AuthException e) {
			throw new StartupException(e.getMessage());
		}
		try {
			authorizer.reset();
		} catch (AuthException e) {
			throw new StartupException(e.getMessage());
		}
		FileNodeManager.getInstance().resetFileNodeManager();
		MultiFileLogNodeManager.getInstance().start();
	}
}
