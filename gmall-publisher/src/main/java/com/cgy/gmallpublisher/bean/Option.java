package com.cgy.gmallpublisher.bean;

/**
 * @author GyuanYuan Cai
 * 2020/10/24
 * Description:
 */

//  表示结果中的一个选项
public class Option {
    private String name;
    private Long value;

    public Option() {

    }

    public Option(String name, Long value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}

