package peaksoft;

import peaksoft.config.Util;
import peaksoft.model.Employee;
import peaksoft.model.Job;
import peaksoft.service.EmployeeService;
import peaksoft.service.Impl.EmployeeServiceImpl;
import peaksoft.service.Impl.JobServiceImpl;
import peaksoft.service.JobService;

import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {

        EmployeeService employeeService = new EmployeeServiceImpl();
        JobService jobService = new JobServiceImpl();

        System.out.println("1: Employees methods. \n" +
                "2: Jobs methods.");
        switch (new Scanner(System.in).nextInt()) {
            case 1:
                while (true) {
                    System.out.println("""
                            1: create table employee
                            2: add employee
                            3: drop table employee
                            4: clear table employee
                            5: update employee
                            6: get all employees
                            7: get employee find by email
                            8: get employee by id
                            9: get employee by position
                            """);

                    switch (new Scanner(System.in).nextInt()) {
                        case 1 -> employeeService.createEmployee();
                        case 2 -> {
                            System.out.print("Enter first name: ");
                            String firstName = new Scanner(System.in).nextLine();
                            System.out.print("Enter last name: ");
                            String lastName = new Scanner(System.in).nextLine();
                            System.out.print("Enter age: ");
                            int age = new Scanner(System.in).nextInt();
                            System.out.print("Enter email: ");
                            String email = new Scanner(System.in).nextLine();
                            System.out.print("Enter job id: ");
                            int jobId = new Scanner(System.in).nextInt();
                            employeeService.addEmployee(new Employee(firstName, lastName, age, email, jobId));
                        }
                        case 3 -> employeeService.dropTable();
                        case 4 -> employeeService.cleanTable();
                        case 5 -> {
                            System.out.print("Enter employee id: ");
                            Long id = new Scanner(System.in).nextLong();
                            System.out.print("Enter new first name: ");
                            String firstName = new Scanner(System.in).nextLine();
                            System.out.print("Enter new last name: ");
                            String lastName = new Scanner(System.in).nextLine();
                            System.out.print("Enter new age: ");
                            int age = new Scanner(System.in).nextInt();
                            System.out.print("Enter new email: ");
                            String email = new Scanner(System.in).nextLine();
                            System.out.print("Enter new job id: ");
                            int jobId = new Scanner(System.in).nextInt();
                            employeeService.updateEmployee(id, new Employee(firstName, lastName, age, email, jobId));
                        }
                        case 6 -> employeeService.getAllEmployees().forEach(System.out::println);
                        case 7 -> {
                            System.out.print("Enter email: ");
                            String email = new Scanner(System.in).nextLine();
                            System.out.println(employeeService.findByEmail(email));
                        }
                        case 8 -> {
                            System.out.print("Enter employee id; ");
                            Long id = new Scanner(System.in).nextLong();
                            System.out.println(employeeService.getEmployeeById(id));
                        }
                        case 9 -> {
                            System.out.print("Enter job position: ");
                            String position = new Scanner(System.in).nextLine();
                            employeeService.getEmployeeByPosition(position).forEach(System.out::println);
                        }
                        default -> System.out.println("Net takoi knopki !!!!");
                    }
                }
            case 2:
                while (true) {
                    System.out.println("""
                            1: create table
                            2: add job
                            3: get job by id
                            4: sort by experience
                            5: get job by employee id
                            6: delete column description
                            """);
                    switch (new Scanner(System.in).nextInt()) {
                        case 1 -> jobService.createJobTable();
                        case 2 -> {
                            System.out.print("Enter position: ");
                            String position = new Scanner(System.in).nextLine();
                            System.out.print("Enter profession: ");
                            String profession = new Scanner(System.in).nextLine();
                            System.out.print("Enter description: ");
                            String description = new Scanner(System.in).nextLine();
                            System.out.print("Enter experience: ");
                            int experience = new Scanner(System.in).nextInt();
                            jobService.addJob(new Job(position, profession, description, experience));
                        }
                        case 3 -> {
                            System.out.print("Enter job id: ");
                            Long id = new Scanner(System.in).nextLong();
                            System.out.println(jobService.getJobById(id));
                        }
                        case 4 -> {
                            System.out.print("Enter ask or desc: ");
                            String ascOrDesc = new Scanner(System.in).nextLine();
                            jobService.sortByExperience(ascOrDesc).forEach(System.out::println);
                        }
                        case 5 -> {
                            System.out.print("Enter employee id: ");
                            Long id = new Scanner(System.in).nextLong();
                            System.out.println(jobService.getJobByEmployeeId(id));
                        }
                        case 6 -> jobService.deleteDescriptionColumn();
                    }
                }
            default:
                System.out.println("Error !!!!!");
        }
    }
}
