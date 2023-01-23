package peaksoft.dao.Impl;

import peaksoft.config.Util;
import peaksoft.dao.JobDao;
import peaksoft.model.Job;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JobDaoImpl implements JobDao {

    private Connection connection;

    public JobDaoImpl() {
        this.connection = Util.getConnection();
    }

    @Override
    public void createJobTable() {
        String sql = """
                create table if not exists jobs(
                id serial primary key,
                position varchar not null,
                profession varchar not null,
                description varchar not null,
                experience int not null
                );
                """;
        try(Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            System.out.println("Table Job created successfully.");
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addJob(Job job) {
        String sql = """
                insert into jobs(position, profession, description, experience)
                values(?,?,?,?);
                """;
        try(PreparedStatement preparedStatement = connection.prepareStatement(sql)){
            preparedStatement.setString(1, job.getPosition());
            preparedStatement.setString(2,job.getProfession());
            preparedStatement.setString(3, job.getDescription());
            preparedStatement.setInt(4,job.getExperience());
            preparedStatement.executeUpdate();
            System.out.println("Job added successfully.");
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public Job getJobById(Long jobId) {
        String sql = "select * from jobs where id = ?";
        try(PreparedStatement preparedStatement = connection.prepareStatement(sql)){
            preparedStatement.setLong(1,jobId);
            ResultSet resultSet = preparedStatement.executeQuery();
            Job job = new Job();
            if (!resultSet.next()){
                System.out.println("Does not exists.");
            }
            job.setId(resultSet.getLong(1));
            job.setPosition(resultSet.getString(2));
            job.setProfession(resultSet.getString(3));
            job.setDescription(resultSet.getString(4));
            job.setExperience(resultSet.getInt(5));
            resultSet.close();
            return job;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }    }

    @Override
    public List<Job> sortByExperience(String ascOrDesc) {
        List<Job> jobs = new ArrayList<>();
        String asc = """
                select * from jobs order by experience;
                """;
        String desc = """
                select * from jobs order by experience desc;
                """;
        if (ascOrDesc != null) {
            switch (ascOrDesc) {
                case "asc" -> {
                    try (PreparedStatement preparedStatement = connection.prepareStatement(asc)) {
                        ResultSet resultSet = preparedStatement.executeQuery();
                        while (resultSet.next()) {
                            Long id = resultSet.getLong("id");
                            String position = resultSet.getString("position");
                            String profession = (resultSet.getString("profession"));
                            String description = (resultSet.getString("description"));
                            int experience = (resultSet.getInt("experience"));
                            jobs.add(new Job(id, position, profession, description, experience));
                        }
                        resultSet.close();
                    } catch (SQLException e) {
                        System.out.println(e.getMessage());
                    }
                }
                case "desc" -> {
                    try (PreparedStatement preparedStatement = connection.prepareStatement(desc);
                         ResultSet resultSet = preparedStatement.executeQuery()){
                        while (resultSet.next()) {
                            Long id = resultSet.getLong("id");
                            String position = resultSet.getString("position");
                            String profession = (resultSet.getString("profession"));
                            String description = (resultSet.getString("description"));
                            int experience = (resultSet.getInt("experience"));
                            jobs.add(new Job(id, position, profession, description, experience));
                        }
                    } catch (SQLException e) {
                        System.out.println(e.getMessage());
                    }
                }
                default -> System.out.println("Error!!!!!");
            }
        }else {
            System.out.println("Is null.");
        }
        return jobs;
    }

    @Override
    public Job getJobByEmployeeId(Long employeeId) {
        Job job1 = new Job();
        String s = """
                select * from employees e
                join jobs j on j.id = e.job_id where e.id = ?
                """;
        try(PreparedStatement preparedStatement = connection.prepareStatement(s)){
            preparedStatement.setLong(1,employeeId);
            preparedStatement.executeQuery();
            ResultSet resultSet = preparedStatement.getResultSet();
            while (resultSet.next()) {
                job1.setId(resultSet.getLong("id"));
                job1.setPosition(resultSet.getString("position"));
                job1.setProfession(resultSet.getString("profession"));
                job1.setDescription(resultSet.getString("description"));
                job1.setExperience(resultSet.getInt("job_id"));
            }
            resultSet.close();

        }catch (SQLException e){
            throw new RuntimeException(e);
        }
        return job1;
    }


    @Override
    public void deleteDescriptionColumn() {
        String s = """
                alter table jobs drop column description;
                """;
        try(Statement statement = connection.createStatement()){
            boolean b = statement.execute(s);
            if (!b){
                System.out.println("Column deleted.");
            }
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }
    }
    }
