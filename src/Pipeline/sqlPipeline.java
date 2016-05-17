package Pipeline;

import java.sql.SQLException;
import java.util.List;

import Utils.JdbcUtils;
import shopInfoSpider.DazongPageProcessor;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

public class sqlPipeline implements Pipeline {
	
	//每次页面请求完之后，都会调用这个处理接口，用于对数据的处理

	@Override
	public void process(ResultItems arg0, Task arg1) {
		List<Object> params = arg0.get("params");
		String sql = arg0.get("sql");
		boolean re = false;
		try {
			if (null != sql && null != params) {
				//更新
				re = JdbcUtils.updateByPreparedStatement(sql, params,
						DazongPageProcessor.dbp.getConnection());
				if (re) {
					System.out.println("success!");
				} else {
					System.out.println("fail!");
				}
			}
			params = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
