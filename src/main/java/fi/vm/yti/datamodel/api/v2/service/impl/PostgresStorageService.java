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

import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.service.StorageService;

@Service
public class PostgresStorageService implements StorageService {

	private static final Logger logger = LoggerFactory.getLogger(PostgresStorageService.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public int storeSchemaFile(String schemaPID, String contentType, byte[] data) {
		return storeFile(schemaPID, contentType, data, ModelType.SCHEMA);
	}

	@Override
	public int storeCrosswalkFile(String crosswalkPID, String contentType, byte[] data) {
		return storeFile(crosswalkPID, contentType, data, ModelType.CROSSWALK);
	}

	private int storeFile(String pid, String contentType, byte[] data, ModelType type) {
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
	public StoredFile retrieveSchemaFile(String schemaPID, long fileID) {
		return jdbcTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con
						.prepareStatement("select content_type, data, id from mscr_files where id = ? and type = ?");
				ps.setLong(1, fileID);
				ps.setString(2, ModelType.SCHEMA.name());
				return ps;
			}
		}, new ResultSetExtractor<StoredFile>() {

			@Override
			public StoredFile extractData(ResultSet rs) throws SQLException, DataAccessException {
				rs.next();
				String contentType = rs.getString(1);
				byte[] data = rs.getBytes(2);
				long fileID = rs.getLong(3);
				return new StoredFile(contentType, data, fileID, ModelType.SCHEMA);
			}
		});
	}

	@Override
	public List<StoredFile> retrieveAllSchemaFiles(String schemaPID) {
		return jdbcTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con
						.prepareStatement("select content_type, data, id, type from mscr_files where pid = ? and type = ?");
				ps.setString(1, schemaPID);
				ps.setString(2, ModelType.SCHEMA.name());
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
					ModelType type = ModelType.valueOf(rs.getString(4));
					files.add(new StoredFile(contentType, data, fileID, type));
				}
				return files;
			}
		});
	}

	@Override
	public List<StoredFile> retrieveAllCrosswalkFiles(String pid) {
		return jdbcTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con
						.prepareStatement("select content_type, data, id, type from mscr_files where pid = ? and type = ?");
				ps.setString(1, pid);
				ps.setString(2, ModelType.CROSSWALK.name());
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
					ModelType type = ModelType.valueOf(rs.getString(4));
					files.add(new StoredFile(contentType, data, fileID, type));
				}
				return files;
			}
		});
	}
}
