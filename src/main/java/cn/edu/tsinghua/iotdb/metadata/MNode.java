package cn.edu.tsinghua.iotdb.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

/**
 * This class is the implementation of Metadata Node where "MNode" is the
 * shorthand of "Metadata Node". One MNode instance represents one node in the
 * Metadata Tree
 * 
 * @author Jinrui Zhang
 *
 */
public class MNode implements Serializable {

	private static final long serialVersionUID = -770028375899514063L;

	// The name of the MNode
	private String name;
	// Whether current node is a leaf in the Metadata Tree
	private boolean isLeaf;
	// Whether current node is Storage Level in the Metadata Tree
	private boolean isStorageLevel;
	// Map for the schema in this storage group
	private Map<String, ColumnSchema> schemaMap;
	private Map<String, Integer> numSchemaMap;
	// Corresponding data file name for current node
	private String dataFileName;
	// Column's Schema for one timeseries represented by current node if current
	// node is one leaf
	private ColumnSchema schema;
	private MNode parent;
	private LinkedHashMap<String, MNode> children;

	public MNode(String name, MNode parent, boolean isLeaf) {
		this.setName(name);
		this.parent = parent;
		this.isLeaf = isLeaf;
		this.isStorageLevel = false;
		if (!isLeaf) {
			children = new LinkedHashMap<>();
		}
	}

	public MNode(String name, MNode parent, TSDataType dataType, TSEncoding encoding) {
		this(name, parent, true);
		this.schema = new ColumnSchema(name, dataType, encoding);
	}

	public boolean isStorageLevel() {
		return isStorageLevel;
	}

	public void setStorageLevel(boolean b) {
		this.isStorageLevel = b;
		if(b){
			schemaMap = new HashMap<>();
			numSchemaMap = new HashMap<>();
		}else{
			numSchemaMap = null;
			schemaMap = null;
		}
	}
	
	public Map<String, ColumnSchema> getSchemaMap() {
		return schemaMap;
	}

	public Map<String, Integer> getNumSchemaMap() {
		return numSchemaMap;
	}

	public boolean isLeaf() {
		return isLeaf;
	}

	public void setLeaf(boolean isLeaf) {
		this.isLeaf = isLeaf;
	}

	public boolean hasChild(String key) {
		if (!isLeaf) {
			return this.children.containsKey(key);
		}
		return false;
	}

	public void addChild(String key, MNode child) {
		if (!isLeaf) {
			this.children.put(key, child);
		}
	}

	public void deleteChild(String key) {
		children.remove(key);
	}

	public MNode getChild(String key) {
		if (!isLeaf) {
			return children.get(key);
		}
		return null;
	}

	/**
	 * @return the count of all leaves whose ancestor is current node
	 */
	public int getLeafCount() {
		if (isLeaf) {
			return 1;
		} else {
			int leafCount = 0;
			for (MNode child : this.children.values()) {
				leafCount += child.getLeafCount();
			}
			return leafCount;
		}
	}

	public String getDataFileName() {
		return dataFileName;
	}

	public void setDataFileName(String dataFileName) {
		this.dataFileName = dataFileName;
	}

	public String toString() {
		return this.getName();
	}

	public ColumnSchema getSchema() {
		return schema;
	}

	public void setSchema(ColumnSchema schema) {
		this.schema = schema;
	}

	public MNode getParent() {
		return parent;
	}

	public void setParent(MNode parent) {
		this.parent = parent;
	}

	public LinkedHashMap<String, MNode> getChildren() {
		return children;
	}

	public void setChildren(LinkedHashMap<String, MNode> children) {
		this.children = children;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
