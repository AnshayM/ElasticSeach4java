package pers.anshay.es;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
/**
 * @author Anshay
 * @createDate 2018年9月13日
 */
public class TestElasticSearch4J {
	private static String indexName = "how2java";
	private static RestHighLevelClient client = new RestHighLevelClient(
			RestClient.builder(new HttpHost("localhost", 9200, "http")));

	public static void main(String[] args) throws IOException {
		// 确保索引存在
		if (!checkExistIndex(indexName)) {
			createIndex(indexName);
		}
		// 准备数据
		// Product product = new Product();
		// product.setId(1);
		// product.setName("product1");
		List<Product> products = ProductUtil.file2list("140k_products.txt");
		System.out.println("准备数据，总计" + products.size() + "条");
		
		batchInsert(products);

		// addDocument(product);
		// // 获取文档
		// getDocument(1);
		// // 修改文档
		// updateDocument(product);
		//
		// getDocument(1);
		//
		// // 删除文档
		//// deleteDocument(1);
		//
		// getDocument(1);

		// if (checkExistIndex(indexName)) {
		// deleteIndex(indexName);
		// }
		// checkExistIndex(indexName);

		// 关闭连接
		client.close();
	}

	/**
	 * 批量插入数据
	 * 
	 * @param products
	 * @throws IOException
	 */
	private static void batchInsert(List<Product> products) throws IOException {
		BulkRequest request = new BulkRequest();
		for (Product p : products) {
			Map<String, Object> map = p.toMap();
			IndexRequest insertRequest = new IndexRequest(indexName, "product", String.valueOf(p.getId())).source(map);
			request.add(insertRequest);
		}
		client.bulk(request);
		System.out.println("批量插入完成");
	}

	private static void deleteDocument(int id) throws IOException {
		DeleteRequest request = new DeleteRequest(indexName, "product", String.valueOf(id));
		client.delete(request);
		System.out.println("已经从ElasticSearch服务器上删除id=" + id + "的文档");
	}

	private static void updateDocument(Product p) throws IOException {
		UpdateRequest request = new UpdateRequest(indexName, "product", String.valueOf(p.getId())).doc("name",
				p.getName());
		client.update(request);
		System.out.println("已经在ElasticSearch服务器修改产品为：" + p);

	}

	/**
	 * 根据id获取文档
	 * 
	 * @param id
	 * @throws IOException
	 */
	private static void getDocument(int id) throws IOException {
		GetRequest request = new GetRequest(indexName, "product", String.valueOf(id));
		GetResponse response = client.get(request);

		if (!response.isExists()) {
			System.out.println("检查服务器上" + "id=" + id + "的文档不存在");
		} else {
			String source = response.getSourceAsString();
			System.out.println("获取服务器上" + "id=" + id + "的文档内容是：");
			System.out.println(source);
		}
	}

	/**
	 * 添加文档
	 * 
	 * @param p
	 * @throws IOException
	 */
	private static void addDocument(Product p) throws IOException {
		Map<String, Object> jsonMap = new HashMap<>();
		jsonMap.put("name", p.getName());
		IndexRequest request = new IndexRequest(indexName, "product", String.valueOf(p.getId())).source(jsonMap);
		client.index(request);
		System.out.println("已经向ElasticSearch服务增加产品" + p);
	}

	private static boolean checkExistIndex(String indexName) throws IOException {
		boolean result = true;
		try {

			OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
			client.indices().open(openIndexRequest).isAcknowledged();

		} catch (ElasticsearchStatusException ex) {
			String m = "Elasticsearch exception [type=index_not_found_exception, reason=no such index]";
			if (m.equals(ex.getMessage())) {
				result = false;
			}
		}
		if (result)
			System.out.println("索引:" + indexName + " 是存在的");
		else
			System.out.println("索引:" + indexName + " 不存在");

		return result;

	}

	private static void deleteIndex(String indexName) throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest(indexName);
		client.indices().delete(request);
		System.out.println("删除了索引：" + indexName);

	}

	private static void createIndex(String indexName) throws IOException {
		// TODO Auto-generated method stub
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		client.indices().create(request);
		System.out.println("创建了索引：" + indexName);
	}

}
