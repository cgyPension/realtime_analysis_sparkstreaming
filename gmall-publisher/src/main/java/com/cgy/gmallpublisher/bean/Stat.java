package com.cgy.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * @author GyuanYuan Cai
 * 2020/10/24
 * Description:
 */

// 封装饼图中的数据
// 代表一张饼图
public class Stat {
    private String title;
    private List<Option> options = new ArrayList<>();

    public void addOption(Option option){
        options.add(option);
    }

    public Stat(String title) {
        this.title = title;
    }

    public Stat() {

    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

}


/**
 * Description:
 */

