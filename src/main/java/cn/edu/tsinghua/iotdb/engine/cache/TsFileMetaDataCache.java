package cn.edu.tsinghua.iotdb.engine.cache;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;

/**
 * This class is used to cache <code>TsFileMetaData</code> of tsfile in IoTDB.
 * 
 * @author liukun
 *
 */
public class TsFileMetaDataCache {

	private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetaDataCache.class);
	/** key: The file path of tsfile */
	private ConcurrentHashMap<String, TsFileMetaData> cache;
	private AtomicLong cacheHintNum = new AtomicLong();
	private AtomicLong cacheRequestNum = new AtomicLong();

	private TsFileMetaDataCache() {
		cache = new ConcurrentHashMap<>();
	}

	/*
	 * Singleton pattern
	 */
	private static class TsFileMetaDataCacheHolder {
		private static final TsFileMetaDataCache INSTANCE = new TsFileMetaDataCache();
	}

	public static TsFileMetaDataCache getInstance() {
		return TsFileMetaDataCacheHolder.INSTANCE;
	}

	public TsFileMetaData get(String path) throws IOException {

		path = path.intern();
		synchronized (path) {
			cacheRequestNum.incrementAndGet();
			if (!cache.containsKey(path)) {
				// read value from tsfile
				TsFileMetaData fileMetaData = TsFileMetadataUtils.getTsFileMetaData(path);
				cache.put(path, fileMetaData);
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Cache didn't hint: the number of requests for cache is {}", cacheRequestNum.get());
				}
				return cache.get(path);
			} else {
				cacheHintNum.incrementAndGet();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(
							"Cache hint: the number of requests for cache is {}, the number of hints for cache is {}",
							cacheRequestNum.get(), cacheHintNum.get());
				}
				return cache.get(path);
			}
		}
	}

	public void remove(String path) {
		cache.remove(path);
	}

	public void clear() {
		cache.clear();
	}
}
