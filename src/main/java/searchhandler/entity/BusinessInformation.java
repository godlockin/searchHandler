package searchhandler.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class BusinessInformation {

    private String provinceCode;

    /**
     * essential information
     */
    private Long serialno;
    private Long correlationNo;
    private String registrationNo;
    private String creditCode;
    private String enterprisesType;
    private Long registeredCapital;
    private String registeredCapitalOrigin;
    private Date businessTimeStart;
    private String registrationAuthority;
    private String registrationStatus;
    private String residence;
    private String enterpriseName;
    private String chargePerson;
    private Date establishmentDate;
    private Date businessTimeEnd;
    private Date approvalDate;
    private Date createTime;
    private Date spiderTime;
    private String businessScope;

    /**
     * key personnel
     */
    private Long kSerialno;
    private String name;
    private String positionChinese;
    private Date kCreateTime;

    /**
     * share holder
     */
    private Long sSerialno;
    private String shareholderName;
    private String shareholderType;
    private BigDecimal payableAmount;
    private BigDecimal payableActualAmount;
    private String shareholderCreditType;
    private String shareholderCreditNo;
    private Date sCreateTime;
}
