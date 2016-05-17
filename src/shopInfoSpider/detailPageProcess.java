package shopInfoSpider;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import Utils.DbPoolConnection;
import Utils.JdbcUtils;
import Utils.toZero;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Html;
import us.codecraft.webmagic.selector.Selectable;

public class detailPageProcess implements PageProcessor {
	public static final int TYPE_KTV = 1;
	public static final int TYPE_FOOD = 2;
	public static final int TYPE_HOTEL = 3;
	public static final int TYPE_MOVIE = 4;
	public static final int TYPE_JSON = 5;
	public static final int TYPE_SHOP = 6;
	public static final int TYPE_FINDDEAL = 7;
	public static final int TYPE_DEALINFO = 8;
	public static final int TYPE_DEALCOMMENT = 9;
	public static final int TYPE_SHOPCOMMENT = 10;

	private static Site site = Site
			.me()
			.setRetryTimes(5)
			.setCycleRetryTimes(3)
			.setSleepTime(1000)
			.setTimeOut(5000)
			.setDomain(".dianping.com")
			.setUserAgent(
					"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36");

	@Override
	public Site getSite() {
		// TODO Auto-generated method stub
		return site;
	}

	public void setSite(Site site) {
		this.site = site;
	}

	@Override
	public void process(Page page) {
		//页面详情爬虫回调函数
		//抓取flag、commentcount、和三级行政
		Request request = page.getRequest();
		Html html = page.getHtml();

		int type = (int) request.getExtra("type");
		int type2 = (int) request.getExtra("type2");

		switch (type) {
		case TYPE_SHOP: {
			//页面详情
			String shopId = (String) request.getExtra("shopId");
			String sqlString = "";
			String temString = "";
			String province = "";
			String city = "";
			String district = "";
			List<String> flags = html.xpath(
					"//div[@class='breadcrumb']/a/text()").all();
			int flagSize = flags.size();
			for (String string : flags) {
				sqlString += string;
				sqlString += "|";
			}
			//利用flag信息，进行三级行政区域处理
			for (int i = 0; i < flagSize; i++) {
				String temFlagString = flags.get(i);
				temFlagString = temFlagString.replace(" ", "");
				if (i == 0) {
					if (temFlagString.contains("K歌")
							|| temFlagString.contains("餐厅")
							|| temFlagString.contains("电影")
							|| temFlagString.contains("酒店")) {
						city = temFlagString.substring(0,
								temFlagString.length() - 2);
					}
					if (temFlagString.contains("休闲娱乐")) {
						city = temFlagString.substring(0,
								temFlagString.length() - 4);
					}
					if ("".equals(city) || null == city) {
					} else {
						if (city.contains("县") || city.equals("区")) {
							district = city;
							city = "";
							break;
						}
					}
				}
				if (i == 1) {
					district = temFlagString;
					if (district.contains("其他")) {
						district = district.substring(0, district.length() - 2);
					}
				}
			}
			temString = toZero
					.toZeroUtil(html
							.xpath("//a[@class='item current']/span[@class='sub-title']/text()")
							.toString());
			if ("0".equals(temString) == false) {
				temString = temString.substring(1, temString.length() - 1);
			}
			//
			Request shopInfoRequest = new Request();
			shopInfoRequest.setUrl(DazongPageProcessor.URLBASE_JSON
					+ DazongPageProcessor.unixTime + "&shopId=" + shopId);
			shopInfoRequest.putExtra("type", TYPE_JSON);
			shopInfoRequest.putExtra("type2", type2);
			shopInfoRequest.putExtra("shopId", shopId);
			shopInfoRequest.putExtra("shopFlag", sqlString);
			shopInfoRequest.putExtra("shopNum", temString);
			shopInfoRequest.putExtra("city", city);
			shopInfoRequest.putExtra("district", district);
			page.addTargetRequest(shopInfoRequest);
			flags = null;
			page.setSkip(true);
			break;
		}
		case TYPE_JSON: {
			//商家信息接口
			String shopId = (String) request.getExtra("shopId");
			String shopFlag = (String) request.getExtra("shopFlag");
			String shopNum = (String) request.getExtra("shopNum");
			String province = "";
			String city = (String) request.getExtra("city");
			String district = (String) request.getExtra("district");
			String cityNameCh = (String) request.getExtra("district");
			String cityNameEn = (String) request.getExtra("district");
			JSONObject totalJsonObject = JSON.parseObject(page.getRawText());
			JSONObject jsonObject = totalJsonObject.getJSONObject("msg")
					.getJSONObject("shopInfo");
			String parkReviewCount = totalJsonObject.getJSONObject("msg")
					.getJSONObject("parkInfo").get("parkReviewCount")
					.toString();
			List<Object> params = new ArrayList<Object>();

			params.add(shopId);
			params.add(jsonObject.get("shopName"));
			params.add(jsonObject.get("branchName"));
			params.add(shopFlag + "");
			params.add(jsonObject.get("address"));
			params.add(jsonObject.get("phoneNo"));
			params.add(jsonObject.get("phoneNo2"));
			String cityId = jsonObject.get("cityId").toString();
			params.add(cityId);
			//用cityId查询cityNameCh、cityNameEn
			//@TODO
			String findCity = "select cityNameCh,cityNameEn,province from dazhongdianping_city where cityId = "
					+ cityId + " limit 1";
			try {
				Map<String, Object> cityInfo = JdbcUtils
						.findSimpleResult(findCity, null,
								DazongPageProcessor.dbp.getConnection());
				province = (String) cityInfo.get("province");
				cityNameEn = (String) cityInfo.get("cityNameEn");
				cityNameCh = (String) cityInfo.get("cityNameCh");
				cityInfo = null;
			} catch (SQLException e) {
				System.out.println("find City error!");
			}
			params.add("" + cityNameCh);
			params.add("" + cityNameEn);
			params.add(toZero.toZeroUtil(jsonObject.get("hits")));
			params.add(toZero.toZeroUtil(jsonObject.get("todayHits")));
			params.add(toZero.toZeroUtil(jsonObject.get("weeklyHits")));
			params.add(toZero.toZeroUtil(jsonObject.get("prevWeeklyHits")));
			params.add(toZero.toZeroUtil(jsonObject.get("monthlyHits")));
			params.add(toZero.toZeroUtil(jsonObject.get("wishTotal")));
			params.add(toZero.toZeroUtil(jsonObject.get("score")));
			params.add(toZero.toZeroUtil(jsonObject.get("score1")));
			params.add(toZero.toZeroUtil(jsonObject.get("score2")));
			params.add(toZero.toZeroUtil(jsonObject.get("score3")));
			params.add(toZero.toZeroUtil(jsonObject.get("avgPrice")));
			params.add(jsonObject.get("writeUp"));
			params.add(toZero.toZeroUtil(jsonObject.get("popularity")));
			params.add(jsonObject.get("glat"));
			params.add(jsonObject.get("glng"));
			params.add(toZero.toZeroUtil(jsonObject.get("shopPower")));
			params.add(jsonObject.get("businessHours"));
			params.add(shopNum);
			params.add(toZero.toZeroUtil(parkReviewCount));
			params.add(province);
			params.add(city);
			params.add(district);

			StringBuffer sql = new StringBuffer("INSERT ignore INTO ");
			switch (type2) {
			case TYPE_FOOD:
				sql.append("`dazongdianping`.`dazhongdianping_shopinfo_single_food_20160507`");
				break;
			case TYPE_MOVIE:
				sql.append("`dazongdianping`.`dazhongdianping_shopinfo_single_movie_20160507`");
				break;
			case TYPE_HOTEL:
				sql.append("`dazongdianping`.`dazhongdianping_shopinfo_single_hotel_20160507`");
				break;
			case TYPE_KTV:
				sql.append("`dazongdianping`.`dazhongdianping_shopinfo_single_ktv_20160507`");
				break;
			default:
				break;
			}
			sql.append("(`shopId`,`shopName`,`branchName`,`tagInfo`,`address`,`phoneNo`,`phoneNo2`,`cityId`,"
					+ "`cityNameCh`,`cityNameEn`,"
					+ "`hits`,`todayHits`,`weeklyHits`,`prevWeeklyHits`,"
					+ "`monthlyHits`,`wishTotal`,`score`,`score1`,`score2`,`score3`,`avgPrice`,"
					+ "`writeUp`,`popularity`,`glat`,`glng`,`shopPower`,`businessHours`,`commentCount`,"
					+ "`parkReviewCount`,`province`,`city`,`district`) values");
			sql = sql
					.append("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			System.out.println(params);
			//将得到的信息params,构建好的sql语句,放到处理类中
			page.putField("params", params);
			page.putField("sql", sql.toString());
			totalJsonObject = null;
			sql = null;
			jsonObject = null;
			params = null;
			break;
		}

		default:
			System.out.println("fail!");
			break;
		}

		request = null;
		html = null;

	}
}
