package com.tigerobo.searchhandler.mapper;

import com.tigerobo.searchhandler.entity.ShareholderInformation;

public interface ShareholderInformationMapper {
    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table shareholder_information
     *
     * @mbg.generated Tue Dec 11 10:20:06 CST 2018
     */
    int deleteByPrimaryKey(Integer serialno);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table shareholder_information
     *
     * @mbg.generated Tue Dec 11 10:20:06 CST 2018
     */
    int insert(ShareholderInformation record);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table shareholder_information
     *
     * @mbg.generated Tue Dec 11 10:20:06 CST 2018
     */
    int insertSelective(ShareholderInformation record);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table shareholder_information
     *
     * @mbg.generated Tue Dec 11 10:20:06 CST 2018
     */
    ShareholderInformation selectByPrimaryKey(Integer serialno);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table shareholder_information
     *
     * @mbg.generated Tue Dec 11 10:20:06 CST 2018
     */
    int updateByPrimaryKeySelective(ShareholderInformation record);

    /**
     * This method was generated by MyBatis Generator.
     * This method corresponds to the database table shareholder_information
     *
     * @mbg.generated Tue Dec 11 10:20:06 CST 2018
     */
    int updateByPrimaryKey(ShareholderInformation record);
}