package pers.anshay.es;


import java.awt.AWTException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;


/**工具类：把数据从文本中导出来
 * @author Anshay
 * @createDate 2018年9月13日
 */
public class ProductUtil {
	


	public static void main(String[] args) throws IOException, InterruptedException, AWTException {

		String fileName = "140k_products.txt";
		
		List<Product> products = file2list(fileName);
		
		System.out.println(products.size());
			
	}



	public static List<Product> file2list(String fileName) throws IOException {
		File f = new File(fileName);
		List<String> lines = FileUtils.readLines(f,"UTF-8");
		List<Product> products = new ArrayList<>();
		for (String line : lines) {
			Product p = line2product(line);
			products.add(p);
		}
		return products;
	}
	


	private static Product line2product(String line) {
		Product p = new Product();
		String[] fields = line.split(",");
		p.setId(Integer.parseInt(fields[0]));
		p.setName(fields[1]);
		p.setCategory(fields[2]);
		p.setPrice(Float.parseFloat(fields[3]));
		p.setPlace(fields[4]);
		p.setCode(fields[5]);
		return p;
	}




}
