package com.tigerobo.searchhandler;

import com.tigerobo.searchhandler.common.utils.DataUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SearchHandlerApplicationTests {

	@Test
	public void contextLoads() {
		System.out.println(DataUtils.formatDate(new Date()));
	}

}
