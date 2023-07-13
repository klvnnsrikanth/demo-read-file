package com.example.demo.cron;


import com.example.demo.entity.User;
import com.example.demo.repository.UserRepository;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;

import static com.fasterxml.jackson.dataformat.csv.CsvSchema.emptySchema;

@Component
public class ReadFileJob {
    @Value("${users.directoryPath}")
    private String usersDirectoryPath;

    private UserRepository userRepository;

    public ReadFileJob(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Scheduled(fixedDelay = 5000)
    public void deleteAllRecordsTask() {
        userRepository.deleteAll();
        System.out.println("Cleared all records");
    }

    @Scheduled(fixedDelay = 10000)
    public void insertAllRecords() throws Exception {
        System.out.println("Inserting All User Records");

        try (Stream<Path> paths = Files.walk(Paths.get(usersDirectoryPath))) {
            paths.filter(Files::isRegularFile).forEach(f -> readFileAndInsertToDB(f.toAbsolutePath().toFile()));
        } catch (Exception ex) {
            System.out.println("Failed to read file and insert data");
        }
    }

    private void readFileAndInsertToDB(File csvFile) {
        MappingIterator<User> userIterator = null;
        try {
            userIterator = new CsvMapper().enable(CsvParser.Feature.SKIP_EMPTY_LINES)
                    .readerWithTypedSchemaFor(User.class)
                    .with(emptySchema().withHeader().withNullValue(""))
                    .readValues(csvFile);
        } catch (IOException e) {
            System.out.println("Failed to read data from csv file and insert data");
        }
        userIterator.forEachRemaining(userRepository::save);
    }
}
