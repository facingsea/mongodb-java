package com.facingsea.mongodbjava.test;

import static java.util.Arrays.asList;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * refer: https://docs.mongodb.com/getting-started/java/
 * @author wangzhf
 *
 */
public class Warm {
	
	static String HOST = "localhost";
	static int PORT = 27017;
	
	@Test
	public void testConnection(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		ListCollectionsIterable<Document> result = db.listCollections();
		Document doc = result.first();
		String val = doc.toString();
		System.out.println(val);
		String json = doc.toJson();
		System.out.println(json);
		
		
		client.close();
	}
	
	@Test
	public void testInsert() throws ParseException{
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		db.getCollection("restaurants").insertOne(new Document(
				"address", new Document()
							.append("street", "北京")
							.append("zipcode", "100086")
							.append("grades", asList(
									new Document()
										.append("date", format.parse("2014-10-01T00:00:00Z"))
										.append("grade", "A")
		                                .append("score", 11),
		                            new Document()
		                                .append("date", format.parse("2014-01-16T00:00:00Z"))
		                                .append("grade", "B")
		                                .append("score", 17)
									))
							.append("name", "facingsea")
			                .append("restaurant_id", "41704620")
				));
		
		client.close();
	}
	
	@Test
	public void testFindAll(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		FindIterable<Document> iterable = db.getCollection("restaurants").find();
		System.out.println(iterable.toString());
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		client.close();
	}
	
	@Test
	public void testFind(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		//FindIterable<Document> iterable = db.getCollection("restaurants").find(new Document("address.zipcode", "100086"));
//		FindIterable<Document> iterable = db.getCollection("restaurants").find(Filters.eq("address.zipcode", "100086"));
		FindIterable<Document> iterable = db.getCollection("restaurants")
				.find(new Document("name", "Morris Park Bake Shop"));
		System.out.println(iterable.toString());
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		client.close();
	}
	
	@Test
	public void testFindInArray(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		FindIterable<Document> iterable = db.getCollection("restaurants")
				.find(new Document("grades.grade", "B"));
		System.out.println(iterable.toString());
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		client.close();
	}
	
	@Test
	public void testFindWithCompare(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
//		FindIterable<Document> iterable = db.getCollection("restaurants")
//				.find(new Document("grades.score", new Document("$gt", 30)));
		FindIterable<Document> iterable = db.getCollection("restaurants")
				.find(Filters.gt("grades.score", 30));
		System.out.println(iterable.toString());
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		client.close();
	}
	
	/**
	 * 多条件查找
	 * 逻辑与
	 */
	@Test
	public void testFindWithCombineConditionAnd(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
//		FindIterable<Document> iterable = db.getCollection("restaurants")
//				.find(new Document("cuisine", "Italian").append("address.zipcode", "10075"));
		FindIterable<Document> iterable = db.getCollection("restaurants")
				.find(Filters.and(Filters.eq("cuisine", "Italian"), Filters.eq("address.zipcode", "10075")));
		System.out.println(iterable.toString());
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		client.close();
	}
	
	/**
	 * 多条件查找
	 * 逻辑或
	 */
	@Test
	public void testFindWithCombineConditionOr(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		FindIterable<Document> iterable = db.getCollection("restaurants")
				.find(new Document("$or", asList(new Document("cuisine", "Italian"), 
						new Document("address.zipcode", "100086"))));
		
//		FindIterable<Document> iterable = db.getCollection("restaurants")
//				.find(Filters.or(Filters.eq("cuisine", "Italian"), Filters.eq("address.zipcode", "100086")));
		System.out.println(iterable.toString());
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		client.close();
	}
	
	/**
	 * 多条件查找
	 * 排序
	 */
	@Test
	public void testFindWithCombineConditionSort(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
//		FindIterable<Document> iterable = db.getCollection("restaurants")
//				.find(new Document("grades.score", new Document("$gt", 90)))
//				.sort(new Document("address.zipcode", 1)); // 1 for ascending and -1 for descending
		
		FindIterable<Document> iterable = db.getCollection("restaurants")
				.find(Filters.gt("grades.score", 90))
				.sort(Sorts.ascending("address.zipcode"));
		System.out.println(iterable.toString());
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		client.close();
	}
	
	/**
	 * 更新一个
	 */
	@Test
	public void testUpdate(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		UpdateResult ur = db.getCollection("restaurants")
				.updateOne(new Document("name", "Morris Park Bake Shop"), 
					new Document("$set", new Document("cuisine", "zhongguo"))
						.append("$currentDate", new Document("lastModified", true))
				);
		
		System.out.println(ur.getModifiedCount());
		
		FindIterable<Document> iterable = db.getCollection("restaurants").find(new Document("cuisine", "zhongguo"));
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		
		client.close();
	}
	
	/**
	 * 更新一个
	 */
	@Test
	public void testUpdateEmbeddedField(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		UpdateResult ur = db.getCollection("restaurants")
				.updateOne(new Document("name", "Morris Park Bake Shop"), 
					new Document("$set", new Document("address.street", "ShangDiXiLu"))
						.append("$currentDate", new Document("lastModified", true))
				);
		
		System.out.println(ur.getModifiedCount());
		
		FindIterable<Document> iterable = db.getCollection("restaurants").find(new Document("address.street", "ShangDiXiLu"));
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		
		client.close();
	}
	
	/**
	 * 更新多个
	 */
	@Test
	public void testUpdateMany(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		UpdateResult ur = db.getCollection("restaurants")
				.updateMany(new Document("address.zipcode", "10016").append("cuisine", "Other"), 
					new Document("$set", new Document("cuisine", "XiErQi"))
						.append("$currentDate", new Document("lastModified", true))
				);
		
		System.out.println(ur.getModifiedCount());
		
		FindIterable<Document> iterable = db.getCollection("restaurants").find(new Document("cuisine", "XiErQi"));
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		
		client.close();
	}
	
	/**
	 * 替换
	 */
	@Test
	public void testReplaceOne(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		UpdateResult ur = db.getCollection("restaurants").replaceOne(new Document("restaurant_id", "30075445"), 
				new Document("address", new Document("building", "1111")
										.append("street", "MaLianWa")
				                        .append("zipcode", "10075")
				                        .append("coord", asList(-73.9557413, 40.7720266))
										)
					.append("name", "zhangsan")
				);
		
		System.out.println(ur.getModifiedCount());
		
		FindIterable<Document> iterable = db.getCollection("restaurants").find(new Document("name", "zhangsan"));
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		
		client.close();
	}
	
	/**
	 * 替换
	 * @not work
	 */
	@Test
	public void testReplaceOneWithUpsert(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		UpdateResult ur = db.getCollection("restaurants").replaceOne(new Document("restaurant_id", "8888888"), 
				new Document("address", new Document("building", "1111")
										.append("street", "MaLianWa")
				                        .append("zipcode", "10075")
				                        .append("coord", asList(-73.9557413, 40.7720266))
						).append("name", "zhangsan")
				, new UpdateOptions().upsert(true)
				);
		
		System.out.println(ur.getModifiedCount());
		
		FindIterable<Document> iterable = db.getCollection("restaurants").find(new Document("restaurant_id", "8888888"));
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		
		client.close();
	}
	
	/**
	 * 更新一个，如果没有就插入
	 */
	@Test
	public void testUpdateWithUpsert(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		UpdateResult ur = db.getCollection("restaurants")
				.updateOne(new Document("name", "wangxiaowu"), 
					new Document("$set", new Document("cuisine", "zhongguo"))
						.append("$currentDate", new Document("lastModified", true))
				, new UpdateOptions().upsert(true));
		
		System.out.println(ur.getModifiedCount());
		
		FindIterable<Document> iterable = db.getCollection("restaurants").find(new Document("name", "wangxiaowu"));
		iterable.forEach(new Block<Document>() {

			@Override
			public void apply(Document t) {
				System.out.println(t);
			}
		});
		
		client.close();
	}
	
	/**
	 * 
	 */
	@Test
	public void testDeleteMany(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		DeleteResult dr = db.getCollection("restaurants").deleteMany(new Document("borough", "Manhattan"));
		// 删除整个文档
		// db.getCollection("restaurants").deleteMany(new Document());
		System.out.println(dr.getDeletedCount());
		
		client.close();
	}
	
	/**
	 * 
	 */
	@Test
	public void testDrop(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		// 删除整个集合
		db.getCollection("restaurants").drop();
		
		client.close();
	}
	
	
	/**
	 *  聚合：分组，统计
	 */
	@Test
	public void testGroup(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		AggregateIterable<Document> ai = db.getCollection("restaurants").aggregate(asList(
				new Document("$group", new Document("_id", "$borough").append("count", new Document("$sum", 1)))
				));
		ai.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document.toJson());
		    }
		});
		
		client.close();
	}
	
	/**
	 *  聚合：分组，统计
	 */
	@Test
	public void testMatch(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		AggregateIterable<Document> ai = db.getCollection("restaurants").aggregate(asList(
				new Document("$match", new Document("borough", "Queens").append("cuisine", "Brazilian")), 
				new Document("$group", new Document("_id", "$address.zipcode").append("count", new Document("$sum", 1)))
				));
		
		
		ai.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document.toJson());
		    }
		});
		
		client.close();
	}
	
	/**
	 *  索引
	 */
	@Test
	public void testIndexs(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		// new Document(<field1>, <type1>).append(<field2>, <type2>) ...
		// For an ascending index type, specify 1 for <type>
		// For a descending index type, specify -1 for <type>
		db.getCollection("restaurants").createIndex(new Document("cuisine", 1));
		
		
		client.close();
	}
	
	/**
	 *  索引
	 */
	@Test
	public void testCompoundIndexs(){
		MongoClient client = new MongoClient(HOST, PORT);
		MongoDatabase db = client.getDatabase("test");
		
		// new Document(<field1>, <type1>).append(<field2>, <type2>) ...
		// For an ascending index type, specify 1 for <type>
		// For a descending index type, specify -1 for <type>
		db.getCollection("restaurants").createIndex(new Document("cuisine", 1).append("address.zipcode", -1));
		
		
		client.close();
	}
	
	

}
