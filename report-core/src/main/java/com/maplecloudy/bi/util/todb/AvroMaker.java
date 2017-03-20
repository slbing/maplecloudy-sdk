package com.maplecloudy.bi.util.todb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

/**
 * @author wangli
 * Create a avro file of a specific schema, call funcion write to
 *  write data to avro file, remember to close it after write;
 *
 */
public class AvroMaker{
	private DataFileWriter<GenericRecord> dataFileWriter;
	private Schema schema = null;
	SchemaTreeNode stree = null;
	
	/**
	 * @param dataFile avro file
	 * @param schemaFile avsc file
	 * @throws IOException
	 */
	public AvroMaker(String dataFile, String schemaFile) throws IOException{
		schema = new Schema.Parser().parse(new File(schemaFile));
		File file = new File(dataFile);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		dataFileWriter.create(schema, file);
		
		stree = new SchemaTreeNode(null,this.schema);	//根节点schema也是一个类型schema，这里field设为null，在这里可以理解为field为avro文件名。
		analyseSchemaNode(stree);
		initTypeMap();
	}
	

	/**
	 * store the map of schema type and java type
	 */
	private static Map<Type, Class<?>> typeMap;
	
	void initTypeMap(){
		typeMap = new HashMap<Schema.Type, Class<?>>();
		typeMap.put(Type.ARRAY, Collection.class);
		typeMap.put(Type.BOOLEAN, Boolean.class);
		typeMap.put(Type.BYTES, java.nio.ByteBuffer.class);
		typeMap.put(Type.DOUBLE, Double.class);
		typeMap.put(Type.ENUM, java.lang.String.class);
		typeMap.put(Type.FIXED, Byte[].class);
		typeMap.put(Type.FLOAT, Float.class);
		typeMap.put(Type.INT, Integer.class);
		typeMap.put(Type.LONG, Long.class);
		typeMap.put(Type.MAP, java.util.Map.class);
		//typeMap.put(Type.NULL, null);		// not supported
		typeMap.put(Type.RECORD, GenericRecord.class);
		typeMap.put(Type.STRING, org.apache.avro.util.Utf8.class);
		typeMap.put(Type.UNION, Object.class);
	}
	
	private boolean checkType(Type type, Object obj){
		Class<?> cls = typeMap.get(type);
		assert(cls!=null);
		
		return cls.isInstance(obj);
	}
	
	/**
	 * schema解析树节点
	 * @author wangli<br>
	 *
	 */
	class SchemaTreeNode{
		/**
		 * 域的名称
		 */
		public String fieldName = null;	
		/**
		 * 该域对应的类型schema描述
		 */
		public Schema typeSchema = null;
		/**
		 * 子域的链表
		 */
		public List<SchemaTreeNode> sons = null;
		/**
		 * 该域信息对应的GenericRecord实例
		 */
		public Object record;	
		/**
		 * 记录record中是否有数据,用于union类型的Record生成
		 */
		public boolean haveData = false;	
		/**
		 * 记录（子）节点是否有空指针异常（某个节点值类型不能为空，但某个父亲结点为union类型，<br>
		 * 并且根据数据解析的结果为该异常结点类型并未生效，此时的异常其实并非异常，不需要抛出），<br>
		 * 并向父亲结点传递，如果传递到根结点，则说明异常的结点生效，解析错误，抛出异常。
		 */
		public NullPointerException ex = null;		
		
		public SchemaTreeNode(String fieldName, Schema typeSchema) {
			this.fieldName = fieldName;
			this.typeSchema = typeSchema;
		}
		
		public SchemaTreeNode(Pair<String,Schema> pair){
			this.fieldName = pair.field;
			this.typeSchema = pair.typeSchema;
		}
	}
	
	protected void clearRecordData(SchemaTreeNode node){
		node.record = null;
		node.haveData = false;
		
		if (node.sons != null){
			for (SchemaTreeNode nodeSub : node.sons){
				clearRecordData(nodeSub);
			}
		}
	}
	
	protected void genRecordDataRecordType(SchemaTreeNode node){
		assert(node.sons != null && node.sons.size()!=0);
		node.record = new GenericData.Record(node.typeSchema);	//A record type node must generate a record
		for (SchemaTreeNode nodeSub : node.sons){
			if (nodeSub.record != null){	//(encoding by UTF-8)每个孩子结点都是根据该孩子结点的数据类型和实际数据生成的，只要其record生成了就肯定是合法的，而与haveData无关！
				((GenericRecord) node.record).put(nodeSub.fieldName, nodeSub.record);
				node.haveData = true;
			}
			if (node.ex==null && nodeSub.ex!=null){
				if (node.fieldName != null){
					node.ex = new NullPointerException(node.fieldName+"."+nodeSub.ex.getMessage());
				}else{
					node.ex = nodeSub.ex;
				}
			}
		}
	}
	
	protected void genRecordDataUnionTypeFromSon(SchemaTreeNode node, SchemaTreeNode nodeSub){
		assert(nodeSub.record != null);
		node.record = nodeSub.record;
		node.haveData = true;
		if (nodeSub.ex!=null){
			if (node.fieldName != null){
				node.ex = new NullPointerException(node.fieldName+"."+nodeSub.ex.getMessage());
			}else{
				node.ex = nodeSub.ex;
			}
		}
	}
	
	protected void genRecordDataUnionType(SchemaTreeNode node){
		assert(node.sons != null && node.sons.size()!=0);
		boolean couldNull = false;
		
		SchemaTreeNode selectedNode = null;	//union类型中将被实例化的类型节电
		for (SchemaTreeNode nodeSub : node.sons){
			if (nodeSub.typeSchema.getType() == Type.NULL){	//this union could be null
				couldNull = true;
			}else if (nodeSub.haveData){		//find a union type
				selectedNode = nodeSub;
				genRecordDataUnionTypeFromSon(node,selectedNode);
				break;
			}else{
				if (selectedNode == null){
					selectedNode = nodeSub;
				}else if(selectedNode.ex!=null && nodeSub.ex==null){
					selectedNode = nodeSub;
				}
			}
		}
		
		if (!node.haveData && !couldNull && selectedNode!=null){
			genRecordDataUnionTypeFromSon(node,selectedNode);
			if (selectedNode.ex != null){
				node.ex = new NullPointerException(node.fieldName + " cannot be null");
			}
		}
	}
	
	protected void genRecordData(SchemaTreeNode node) throws UnkownSchemaTypeException{
		if(node.sons == null){
			if (Type.NULL!=node.typeSchema.getType() && node.record==null){
				node.ex = new NullPointerException(node.fieldName + " cannot be null!");
			}
			return ;
		}else{
			for (SchemaTreeNode nodeSub : node.sons){
				genRecordData(nodeSub);
			}

			// scheme type must be record or union
			if (Type.RECORD == node.typeSchema.getType()){
				genRecordDataRecordType(node);
			}else if(Type.UNION == node.typeSchema.getType()){
				genRecordDataUnionType(node);
			}else{
				throw new UnkownSchemaTypeException("Schema type is " + node.typeSchema.getType());
			}
//System.out.println("Field name: " + node.fieldName + "; have data: " + node.haveData + "; record="+node.record +"; Type: "+node.typeSchema);
		}
	}
	
	/**
	 * @param values
	 * @return Number of values that write to avro file successfully
	 */
	public int write(List<Map<String,Object>> values){
		int numSucc = 0;
		if (values != null){
			for(Map<String,Object> value : values){
				if (value == null){
					writeExceptionLog(value,"the value should not be null");
				}else{
					try {
						write(value);
						numSucc ++ ;
					} catch (Exception e) {
						writeExceptionLog(value, e.getClass().getName() + ": "+ e.getMessage());
					}
				}
			}
		}

		return numSucc;
	}
	
	protected void writeExceptionLog(Map<String,Object> errValue, String reason){
		// we just print it to std-output here
		System.out.println("Write avro file error of value: '" + errValue + "' for Reason: " + reason);
	}
	
	
	/**
	 * @param values The map of <strong>path</strong> and value<br>
	 * <strong>path</strong>: for example, a.b.c, and it's Case Insensitive <br>
	 * Note: To know what path is, fistly, we need to know what field type and field name are.<br>
	 * schema describes a kind of data struct, so schema is a data type, and field type is a schema,<br>
	 * each field have a field type, and may be have a field name, a field type may be have a name, <br>
	 * we call it type name. A field type may have several sub-field.<br>
	 * Now we see path "a.b.c", a,b,c are all filed name, it cannot be filed type name except when:<br>
	 * a field's parent field's type is Type.UNION, this field must have no field name, we must use<br>
	 * it's field type's type name as field name which is used to path. 
	 * @throws IOException
	 * @throws TypeMissMatchException 
	 * @throws PathNotFoundException 
	 * @throws UnkownSchemaTypeException 
	 */
	public void write(Map<String,Object> values) throws IOException, TypeMissMatchException, PathNotFoundException, UnkownSchemaTypeException{
		if (values == null){
			return;
		}
		
		clearRecordData(this.stree);	//清空tree数据
		setLeafData(values);	// 给tree的叶子结点赋值

		genRecordData(this.stree);	//生成tree的中间结点
//System.out.println("after gen record==============================================================================");
//printNode(stree,0);
//System.out.println("end==============================================================================");		
//System.out.println(stree.record);
		if (this.stree.ex != null){
			throw this.stree.ex;
		}
		
		dataFileWriter.append((GenericRecord) stree.record);	//写文件

	}
	
	private void setLeafData(Map<String, Object> values) throws TypeMissMatchException, PathNotFoundException {
		for(Map.Entry<String, Object> value : values.entrySet()){
			String[] path = value.getKey().split("\\.");
			
			SchemaTreeNode leaf = findTreeNode(path, this.stree, 0);
			if (leaf == null){
				throw new PathNotFoundException("Warning: Can not find path: "+ value.getKey());
			}else{
				try{
					setData(leaf, value.getValue());
				}catch (TypeMissMatchException ex){
					throw new TypeMissMatchException("Path '"+value.getKey()+"': " + ex.getMessage());
				}
			}
		}
	}


	/**
	 * @param leaf 叶子节点
	 * @param value 需要赋予的值
	 * 向叶子结点赋值
	 * @throws TypeMissMatchException 
	 */
	@SuppressWarnings("unchecked")
	private void setData(SchemaTreeNode leaf, Object value) throws TypeMissMatchException {
		/*
		 * 叶子结点的种类可能包括以下类型：
		 * 基本数据类型;
		 * Type.ARRAY;
		 * Type.BYTES;
		 * Type.ENUM;
		 * Type.FIXED;
		 * Type.MAP;
		 * Type.NULL;
		 */
		assert(leaf != null);
		if (value == null){
			return;
		}
		if (!checkType(leaf.typeSchema.getType(), value)){
			throw new TypeMissMatchException("Schema Type '"+leaf.typeSchema.getType()+"' can not match java type'"+value.getClass().getName()+"', java type must be "+typeMap.get(leaf.typeSchema.getType()).getName());
		}
		
		if (Type.FIXED == leaf.typeSchema.getType()){
			leaf.record = new GenericData.Fixed(leaf.typeSchema, (byte[]) value);
		}else if (Type.ARRAY == leaf.typeSchema.getType()){
			leaf.record = new GenericData.Array<Object>(leaf.typeSchema, (Collection<Object>) value);
		}else{
			leaf.record = value;
		}
		leaf.haveData = true;
	}

	/**
	 * @param path	不区分大小写
	 * @param node
	 * @param pathStep
	 * @return
	 */
	private SchemaTreeNode findTreeNode(String[] path, SchemaTreeNode node, int pathStep) {
		if (pathStep > path.length-1){ // 没找到
			return null;
		}
		if (node.sons == null){	//path过长
			return null;
		}
		
		SchemaTreeNode nodeFind = null;
		
		for (SchemaTreeNode nodeSub : node.sons){
			if (path[pathStep].toUpperCase().equals(nodeSub.fieldName.toUpperCase())){
				nodeFind = nodeSub;
				break;
			}
		}
		
		if (nodeFind != null){
			if (pathStep == path.length-1){
				return nodeFind;
			}else{
				return findTreeNode(path, nodeFind, pathStep+1);
			}
		}else{
			return null;
		}
	}

	public void close() throws IOException{
		dataFileWriter.close();
	}
	
	protected void analyseSchemaNode(SchemaTreeNode node){
		
		List<Pair<String,Schema>> lsts = analyseSchema(node.typeSchema);
		
		if (lsts.size() > 0){
			node.sons = new ArrayList<AvroMaker.SchemaTreeNode>();
			for(Pair<String,Schema> sc : lsts){
				SchemaTreeNode nodeSub = new SchemaTreeNode(sc);
				node.sons.add(nodeSub);
				analyseSchemaNode(nodeSub);
			}
		}
	}
	
	class Pair<FD,TS>{
		FD field = null;
		TS typeSchema = null;
		
		
		public Pair(FD field, TS typeSchema) {
			this.field = field;
			this.typeSchema = typeSchema;
		}
	};
	
	protected List<Pair<String,Schema>> analyseSchema(Schema sch){
		List<Pair<String,Schema>> ret = new ArrayList<AvroMaker.Pair<String,Schema>>();

		if (Type.UNION == sch.getType()){
			for(Schema schSub : sch.getTypes()){
				ret.add(new Pair<String, Schema>(schSub.getName().toLowerCase(),schSub));		//use type name as field name
			}
		}else if(Type.MAP == sch.getType()){
			//ret.add(sch.getValueType());		//must be a java type! if not, not supported!
		}else if (Type.RECORD == sch.getType()){
			for(Field field : sch.getFields()){
				ret.add(new Pair<String, Schema>(field.name(), field.schema()));
			}
		}else if(Type.ARRAY ==  sch.getType()){
			//ret.add(sch.getElementType());	//must be a java type! if not, not supported!
		}else{
			// have no son node(s)
		}

		return ret;
	}
	
	public void printSchemaTree(){
		printNode(stree, 0);
	}
	
	protected void printBlank(int n){
		for(int i=0;i<n;++i){
			if (i%TAB == n%TAB){
				System.out.print('|');
			}else{
				System.out.print(' ');
			}
		}
	}
	private static final int TAB = 4;
	
	protected void printNode(SchemaTreeNode node, int indent){
		Schema sch = node.typeSchema;
		printBlank(indent);
		System.out.println("Field name: "+ node.fieldName + 
				"; Type name: " + sch.getFullName() + 
				"; Type schema type: " + sch.getType());
		if(this.stree.record != null){
			printBlank(indent);
			System.out.println("Record: "+ node.record);
			if (node.ex != null){
				printBlank(indent);
				System.out.println("Exception: "+ node.ex.getMessage());
			}
		}
		
		printBlank(indent);
		System.out.println("Type schema: "+sch);
		
		if (node.sons == null){
			printBlank(indent+TAB);
			System.out.println("[HAVE NO SUB-NODE]");
		}else{
			for (SchemaTreeNode nodeSub: node.sons){
				printNode(nodeSub, indent+TAB);
			}
		}
	}
	
	class TypeMissMatchException extends Exception{
		private static final long serialVersionUID = -6380688510593477912L;

		public TypeMissMatchException(String message) {
			super(message);
		}
	}
	
	class PathNotFoundException extends Exception{
		private static final long serialVersionUID = -1583892684795542595L;

		public PathNotFoundException(String message) {
			super(message);
		}
	}
	
	class UnkownSchemaTypeException extends Exception{
		private static final long serialVersionUID = -4942732266209002452L;

		public UnkownSchemaTypeException(String message) {
			super(message);
		}
	}
	
	
	/*
	 * The schema in dataFile is: 
	 * {"type":"record","name":"Pair","namespace":"com.maplecloudy.avro.io","fields":[{"name":"key","type":{"type":"record","name":"UnionData","fields":[{"name":"datum","type":["null",{"type":"record","name":"UserScenarioKey","namespace":"com.maplecloudy.bi.report.modle.ScenarioKey$","fields":[{"name":"appuid","type":["null","string"],"default":null},{"name":"appid","type":["null","string"],"default":null},{"name":"gender","type":["null","int"],"default":null},{"name":"scenario","type":["null","string"],"default":null}]},{"type":"record","name":"ScenarioKey","namespace":"com.maplecloudy.bi.report.modle","fields":[{"name":"appid","type":["null","string"],"default":null},{"name":"gender","type":["null","int"],"default":null},{"name":"scenario","type":["null","string"],"default":null}]}]}]},"doc":""},{"name":"value","type":{"type":"record","name":"MyAvroValueType","namespace":"com.maplecloudy.bi.util.todb.test","fields":[{"name":"values","type":{"type":"map","values":"long"}}]},"doc":"","order":"ignore"}]}
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) throws IOException{
		
		if (false){
		String dataFile = "quickly-report/report/Hourly/2013-07-25/00/Scenario/current3/-r-00000/data.avro";
		String schemaFile = "quickly-report/report/Hourly/2013-07-25/00/Scenario/current3/-r-00000/data.avsc";

		AvroMaker avermaker = new AvroMaker(dataFile, schemaFile);
		//avermaker.printSchemaTree();
		
		Map<String,Object> row = new HashMap<String, Object>();
		//row.put("key.datum.ScenarioKey.appid.string", new Utf8("myappid"));
		//row.put("key.datum.ScenarioKey.gender.int", 3);
		
		Map<String, Long> map = new HashMap<String, Long>();
		map.put("key1", (long) 1);
		map.put("key2", (long) 2);
		row.put("value.values", map);
		
		try {
			avermaker.write(row);
		} catch (TypeMissMatchException e) {
			e.printStackTrace();//TODO to see
		} catch (PathNotFoundException e) {
			e.printStackTrace();
		} catch (UnkownSchemaTypeException e) {
			e.printStackTrace();
		} catch (NullPointerException e){
			e.printStackTrace();
		}
		
		
		//avermaker.printSchemaTree();
		avermaker.close();
		}
		
		if (true){
			String dataFile = "quickly-report/report/Hourly/2013-07-25/00/Scenario/current4/-r-00000/data.avro";
			String schemaFile = "quickly-report/report/Hourly/2013-07-25/00/Scenario/current4/-r-00000/data.avsc";

			AvroMaker avermaker = new AvroMaker(dataFile, schemaFile);
			avermaker.printSchemaTree();
			
			Map<String,Object> row = new HashMap<String, Object>();
			row.put("key", 3);
			
			Map<String, Long> map = new HashMap<String, Long>();
			map.put("key1", (long) 1);
			map.put("key2", (long) 2);
			row.put("value.values", map);
			
			try {
				avermaker.write(row);
			} catch (TypeMissMatchException e) {
				e.printStackTrace();
			} catch (PathNotFoundException e) {
				e.printStackTrace();
			} catch (UnkownSchemaTypeException e) {
				e.printStackTrace();
			}
			
			avermaker.close();
		}
	}
	
}
