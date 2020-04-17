package sqlToSparkDF.com.tutorial;

public class Employee {

    private String companyName;
    private String address;
    private int totalEmployee;
    private String website;

    public Employee(String companyName, String address, int totalEmployee, String website) {
        this.companyName = companyName;
        this.address = address;
        this.totalEmployee = totalEmployee;
        this.website = website;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getTotalEmployee() {
        return totalEmployee;
    }

    public void setTotalEmployee(int totalEmployee) {
        this.totalEmployee = totalEmployee;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }


}
