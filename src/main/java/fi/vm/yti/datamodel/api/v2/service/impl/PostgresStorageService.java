package fi.vm.yti.datamodel.api.v2.service.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.service.StorageService;

@Service
public class PostgresStorageService implements StorageService {

	private static final Logger logger = LoggerFactory.getLogger(PostgresStorageService.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public int storeSchemaFile(String schemaPID, String contentType, byte[] data) {
		return storeFile(schemaPID, contentType, data, MSCRType.SCHEMA);
	}

	@Override
	public int storeCrosswalkFile(String crosswalkPID, String contentType, byte[] data) {
		return storeFile(crosswalkPID, contentType, data, MSCRType.CROSSWALK);
	}

	private int storeFile(String pid, String contentType, byte[] data, MSCRType type) {
		KeyHolder keyHolder = new GeneratedKeyHolder();

		jdbcTemplate.update(con -> {
			PreparedStatement ps = con.prepareStatement(
					"insert into mscr_files(pid, content_type, data, type) values(?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, pid);
			ps.setString(2, contentType);
			ps.setBytes(3, data);
			ps.setString(4, type.name());
			return ps;
		}, keyHolder);

		return (int) keyHolder.getKeys().get("id");
	}

	@Override
	public StoredFile retrieveFile(String pid, long fileID, MSCRType type) {
		return jdbcTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con
						.prepareStatement("select content_type, data from mscr_files where pid = ? and id = ? and type = ?");
				ps.setString(1, pid);
				ps.setLong(2, fileID);
				ps.setString(3, type.name());
				return ps;
			}
		}, new ResultSetExtractor<StoredFile>() {

			@Override
			public StoredFile extractData(ResultSet rs) throws SQLException, DataAccessException {
				rs.next();
				String contentType = rs.getString(1);
				byte[] data = rs.getBytes(2);
				return new StoredFile(contentType, data, fileID, type);
			}
		});
	}

	@Override
	public List<StoredFile> retrieveAllSchemaFiles(String pid) {
		return retrieveAllFiles(pid, MSCRType.SCHEMA);
	}
	
	@Override
	public List<StoredFile> retrieveAllCrosswalkFiles(String pid) {
		return retrieveAllFiles(pid, MSCRType.CROSSWALK);
	}

	private List<StoredFile> retrieveAllFiles(String pid, MSCRType type) {
		return jdbcTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con
						.prepareStatement("select content_type, data, id from mscr_files where pid = ? and type = ?");
				ps.setString(1, pid);
				ps.setString(2, type.name());
				return ps;
			}
		}, new ResultSetExtractor<List<StoredFile>>() {

			@Override
			public List<StoredFile> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<StoredFile> files = new ArrayList<StoredFile>();
				while (rs.next()) {
					String contentType = rs.getString(1);
					byte[] data = rs.getBytes(2);
					long fileID = rs.getLong(3);
					files.add(new StoredFile(contentType, data, fileID, type));
				}
				return files;
			}
		});
	}

	@Override
	public List<StoredFileMetadata> retrieveAllSchemaFilesMetadata(String pid) {
		return retrieveAllFilesMetadata(pid, MSCRType.SCHEMA);
	}
	
	@Override
	public List<StoredFileMetadata> retrieveAllCrosswalkFilesMetadata(String pid) {
		return retrieveAllFilesMetadata(pid, MSCRType.CROSSWALK);
	}

	private List<StoredFileMetadata> retrieveAllFilesMetadata(String pid, MSCRType type) {
		return jdbcTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con
						.prepareStatement("select content_type, length(data) as size, id from mscr_files where pid = ? and type = ?");
				ps.setString(1, pid);
				ps.setString(2, type.name());
				return ps;
			}
		}, new ResultSetExtractor<List<StoredFileMetadata>>() {

			@Override
			public List<StoredFileMetadata> extractData(ResultSet rs) throws SQLException, DataAccessException {
				List<StoredFileMetadata> files = new ArrayList<StoredFileMetadata>();
				while (rs.next()) {
					String contentType = rs.getString(1);
					int size = rs.getInt(2);
					long fileID = rs.getLong(3);
					files.add(new StoredFileMetadata(contentType, size, fileID, type));
				}
				return files;
			}
		});
	}
	
	@Override
	public void removeFile(long fileID) throws DataAccessException {
		jdbcTemplate.update(con -> {
			PreparedStatement ps = con.prepareStatement("delete from mscr_files where id = ?");
			ps.setLong(1, fileID);
			return ps;
		});
	}

	@Override
	public void deleteAllCrosswalkFiles(String pid) {
		List<StoredFileMetadata> md = retrieveAllCrosswalkFilesMetadata(pid);
		md.forEach(m -> {
			removeFile(m.fileID());
		});
	}

	@Override
	public void deleteAllSchemaFiles(String pid) {
		List<StoredFileMetadata> md = retrieveAllSchemaFilesMetadata(pid);
		md.forEach(m -> {
			removeFile(m.fileID());
		});		
	}
		
}
