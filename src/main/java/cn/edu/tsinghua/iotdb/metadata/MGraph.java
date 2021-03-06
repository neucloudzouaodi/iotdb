package cn.edu.tsinghua.iotdb.metadata;

import java.io.Serializable;
import java.util.*;

import cn.edu.tsinghua.iotdb.exception.MetadataArgsErrorException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;

/**
 * Metadata Graph consists of one {@code MTree} and several {@code PTree}
 * 
 * @author Jinrui Zhang
 *
 */
public class MGraph implements Serializable {
	private static final long serialVersionUID = 8214849219614352834L;

	private MTree mTree;
	private HashMap<String, PTree> pTreeMap;
	private final String separator = "\\.";

	public MGraph(String MTreeName) {
		mTree = new MTree(MTreeName);
		pTreeMap = new HashMap<>();
	}

	/**
	 * Add a {@code PTree} to current {@code MGraph}
	 * @throws MetadataArgsErrorException
	 */
	public void addAPTree(String pTreeRootName) throws MetadataArgsErrorException {
		if(pTreeRootName.toLowerCase().equals("root")){
			throw new MetadataArgsErrorException("Property Tree's root name should not be 'root'");
		}
		PTree pTree = new PTree(pTreeRootName, mTree);
		pTreeMap.put(pTreeRootName, pTree);
	}

	/**
	 * Add a path to Metadata Tree
	 * 
	 * @param path Format: root.node.(node)*
	 * @return The count of new nodes added
	 * @throws MetadataArgsErrorException
	 * @throws PathErrorException
	 */
	public void addPathToMTree(String path, String dataType, String encoding, String args[])
			throws PathErrorException, MetadataArgsErrorException {
		String nodes[] = path.trim().split(separator);
		if (nodes.length == 0) {
			throw new PathErrorException("Timeseries is null");
		}
		mTree.addTimeseriesPath(path, dataType, encoding, args);
	}

	/**
	 * Add a path to {@code PTree}
	 */
	public void addPathToPTree(String path) throws PathErrorException, MetadataArgsErrorException {
		String nodes[] = path.trim().split(separator);
		if (nodes.length == 0) {
			throw new PathErrorException("Timeseries is null.");
		}
		String rootName = path.trim().split(separator)[0];
		if (pTreeMap.containsKey(rootName)) {
			PTree pTree = pTreeMap.get(rootName);
			pTree.addPath(path);
		} else {
			throw new PathErrorException("Timeseries's root is not Correct. RootName: " + rootName);
		}
	}

	/**
	 * Delete path in current MGraph.
	 * @param path a path belongs to MTree or PTree
	 * @throws PathErrorException
	 */
	public String deletePath(String path) throws PathErrorException {
		String nodes[] = path.trim().split(separator);
		if (nodes.length == 0) {
			throw new PathErrorException("Timeseries is null");
		}
		String rootName = path.trim().split(separator)[0];
		if (mTree.getRoot().getName().equals(rootName)) {
			return mTree.deletePath(path);
		} else if (pTreeMap.containsKey(rootName)) {
			PTree pTree = pTreeMap.get(rootName);
			pTree.deletePath(path);
			return null;
		} else {
			throw new PathErrorException("Timeseries's root is not Correct. RootName: " + rootName);
		}
	}

	/**
	 * Link a {@code MNode} to a {@code PNode} in current PTree
	 */
	public void linkMNodeToPTree(String path, String mPath) throws PathErrorException {
		String pTreeName = path.trim().split(separator)[0];
		if (!pTreeMap.containsKey(pTreeName)) {
			throw new PathErrorException("Error: PTree Path Not Correct. Path: " + path);
		} else {
			pTreeMap.get(pTreeName).linkMNode(path, mPath);
		}
	}

	/**
	 * Unlink a {@code MNode} from a {@code PNode} in current PTree
	 */
	public void unlinkMNodeFromPTree(String path, String mPath) throws PathErrorException {
		String pTreeName = path.trim().split(separator)[0];
		if (!pTreeMap.containsKey(pTreeName)) {
			throw new PathErrorException("Error: PTree Path Not Correct. Path: " + path);
		} else {
			pTreeMap.get(pTreeName).unlinkMNode(path, mPath);
		}
	}

	/**
	 * Set storage level for current Metadata Tree.
	 * @param path Format: root.node.(node)*
	 * @throws PathErrorException
	 */
	public void setStorageLevel(String path) throws PathErrorException {
		mTree.setStorageGroup(path);
	}

	/**
	 * Get all paths for given path regular expression if given path belongs to
	 * MTree, or get all linked path for given path if given path belongs to
	 * PTree Notice: Regular expression in this method is formed by the
	 * amalgamation of path and the character '*'
	 * 
	 * @return A HashMap whose Keys are separated by the storage file name.
	 */
	public HashMap<String, ArrayList<String>> getAllPathGroupByFilename(String path) throws PathErrorException {
		String rootName = path.trim().split(separator)[0];
		if (mTree.getRoot().getName().equals(rootName)) {
			return mTree.getAllPath(path);
		} else if (pTreeMap.containsKey(rootName)) {
			PTree pTree = pTreeMap.get(rootName);
			return pTree.getAllLinkedPath(path);
		}
		throw new PathErrorException("Timeseries's root is not Correct. RootName: " + rootName);
	}	
	
	/**
	 * Get all DeltaObject type in current Metadata Tree
	 * @return a HashMap contains all distinct DeltaObject type separated by
	 *         DeltaObject Type
	 */
	public Map<String, List<ColumnSchema>> getSchemaForAllType() throws PathErrorException {
		Map<String, List<ColumnSchema>> res = new HashMap<>();
		List<String> typeList = mTree.getAllType();
		for (String type : typeList) {
			res.put(type, getSchemaForOneType("root." + type));
		}
		return res;
	}

	private ArrayList<String> getDeltaObjectForOneType(String type) throws PathErrorException {
		return mTree.getDeltaObjectForOneType(type);
	}

	/**
	 * Get all delta objects group by DeltaObject type
	 */
	public Map<String, List<String>> getDeltaObjectForAllType() throws PathErrorException {
		Map<String, List<String>> res = new HashMap<>();
		ArrayList<String> types = mTree.getAllType();
		for (String type : types) {
			res.put(type, getDeltaObjectForOneType(type));
		}
		return res;
	}

	/**
	 * Get the full Metadata info.
	 * @return A {@code Metadata} instance which stores all metadata info
	 */
	public Metadata getMetadata() throws PathErrorException {
		Map<String, List<ColumnSchema>> seriesMap = getSchemaForAllType();
		Map<String, List<String>> deltaObjectMap = getDeltaObjectForAllType();
		Metadata metadata = new Metadata(seriesMap, deltaObjectMap);
		return metadata;
	}

	public HashSet<String> getAllStorageGroup() throws PathErrorException {
		return mTree.getAllStorageGroup();
	}

	public List<String> getLeafNodePathInNextLevel(String path) throws PathErrorException {
		return mTree.getLeafNodePathInNextLevel(path);
	}

	/**
	 * Get all ColumnSchemas for given delta object type
	 * @param path A path represented one Delta object
	 * @return a list contains all column schema
	 */
	public ArrayList<ColumnSchema> getSchemaForOneType(String path) throws PathErrorException {
		return mTree.getSchemaForOneType(path);
	}
	
	/**
	 * <p>Get all ColumnSchemas for the filenode path</p>
	 * @param path
	 * @return ArrayList<ColumnSchema> The list of the schema
	 */
	public ArrayList<ColumnSchema> getSchemaForOneFileNode(String path){
		return mTree.getSchemaForOneFileNode(path);
	}
	
	public Map<String, ColumnSchema> getSchemaMapForOneFileNode(String path){
		return mTree.getSchemaMapForOneFileNode(path);
	}
	
	public Map<String, Integer> getNumSchemaMapForOneFileNode(String path){
		return mTree.getNumSchemaMapForOneFileNode(path);
	}

	/**
	 * Calculate the count of storage-level nodes included in given path
	 * @return The total count of storage-level nodes.
	 */
	public int getFileCountForOneType(String path) throws PathErrorException {
		return mTree.getFileCountForOneType(path);
	}

	/**
	 * Get the file name for given path Notice: This method could be called if
	 * and only if the path includes one node whose {@code isStorageLevel} is
	 * true
	 */
	public String getFileNameByPath(String path) throws PathErrorException {
		return mTree.getFileNameByPath(path);
	}

	public String getFileNameByPath(MNode node, String path) throws PathErrorException {
		return mTree.getFileNameByPath(node, path);
	}

	public String getFileNameByPathWithCheck(MNode node, String path) throws PathErrorException {
		return mTree.getFileNameByPathWithCheck(node, path);
	}
	
	public boolean checkFileNameByPath(String path){
		return mTree.checkFileNameByPath(path);
	}

	/**
	 * Check whether the path given exists
	 */
	public boolean pathExist(String path) {
		return mTree.isPathExist(path);
	}

	public boolean pathExist(MNode node, String path) {
		return mTree.isPathExist(node, path);
	}

	public MNode getNodeByPath(String path) throws PathErrorException {
		return mTree.getNodeByPath(path);
	}

	public MNode getNodeByPathWithCheck(String path) throws PathErrorException {
		return mTree.getNodeByPathWithFileLevelCheck(path);
	}

	/**
	 * Extract the DeltaObjectId from given path
	 * @return String represents the DeltaObjectId
	 */
	public String getDeltaObjectTypeByPath(String path) throws PathErrorException {
		return mTree.getDeltaObjectTypeByPath(path);
	}
	
	/**
	 * Get ColumnSchema for given path. Notice: Path must be a complete Path
	 * from root to leaf node.
	 */
	public ColumnSchema getSchemaForOnePath(String path) throws PathErrorException {
		return mTree.getSchemaForOnePath(path);
	}

	public ColumnSchema getSchemaForOnePath(MNode node, String path) throws PathErrorException {
		return mTree.getSchemaForOnePath(node, path);
	}

	public ColumnSchema getSchemaForOnePathWithCheck(MNode node, String path) throws PathErrorException {
		return mTree.getSchemaForOnePathWithCheck(node, path);
	}

	public ColumnSchema getSchemaForOnePathWithCheck(String path) throws PathErrorException {
		return mTree.getSchemaForOnePathWithCheck(path);
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("===  Timeseries Tree  ===\n\n");
		sb.append(mTree.toString());
		return sb.toString();
	}
}
