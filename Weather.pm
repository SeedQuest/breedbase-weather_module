package SGN::Controller::AJAX::Weather;

use Moose;
use Data::Dumper;
use JSON;
use Try::Tiny;
use LWP::UserAgent;
use URI::Escape;
use Digest::SHA qw(hmac_sha256_hex);

BEGIN { extends 'Catalyst::Controller::REST' }

__PACKAGE__->config(
    default   => 'application/json',
    stash_key => 'rest',
    map       => { 'application/json' => 'JSON' },
);

# ============================================================================
# CROPS ENDPOINT
# ============================================================================

sub get_crops : Path('/ajax/weather/crops') Args(0) ActionClass('REST') { }
sub get_crops_GET {
    my ($self, $c) = @_;
    
    my @crops = (
        { id => 1, crop_name => 'Corn (GDD)', base_temp => 10, use_chu => 0 },
        { id => 2, crop_name => 'Corn (CHU)', base_temp => 4.4, use_chu => 1 },
        { id => 3, crop_name => 'Wheat', base_temp => 0, use_chu => 0 },
        { id => 4, crop_name => 'Soybean (GDD)', base_temp => 10, use_chu => 0 },
        { id => 5, crop_name => 'Soybean (CHU)', base_temp => 4.4, use_chu => 1 },
        { id => 6, crop_name => 'Sunflower', base_temp => 8, use_chu => 0 },
        { id => 7, crop_name => 'Custom', base_temp => 10, use_chu => 0 },
    );
    
    $c->stash->{rest} = { crops => \@crops };
}

# ============================================================================
# GDD CALCULATION ENDPOINT
# ============================================================================

sub calculate_gdd : Path('/ajax/weather/gdd') Args(0) ActionClass('REST') { }
sub calculate_gdd_GET { shift->_do_gdd_calculation(@_); }
sub calculate_gdd_POST { shift->_do_gdd_calculation(@_); }

sub _do_gdd_calculation {
    my ($self, $c) = @_;
    
    my $location_id = $c->req->param('location_id');
    my $seasons_json = $c->req->param('seasons');
    my $base_temp = $c->req->param('base_temp') || 10;
    my $data_source = $c->req->param('data_source') || 'openmeteo';
    
    try {
        my $seasons = decode_json($seasons_json);
        my @results;
        my ($lat, $lon) = $self->_get_location_coords($c, $location_id);
        my $cache_stats = { from_cache => 0, from_api => 0 };
        
        foreach my $season (@$seasons) {
            my $year = $season->{year};
            my $start_date = $season->{start_date} || $season->{start} || "$year-04-15";
            my $end_date = $season->{end_date} || $season->{end} || "$year-09-30";
            
            # Try to get cached data first
            my $weather_data = $self->_get_cached_weather($c, $location_id, $start_date, $end_date);
            my $used_cache = 0;
            
            if ($weather_data && scalar(@$weather_data) > 0) {
                $cache_stats->{from_cache} += scalar(@$weather_data);
                $used_cache = 1;
            } else {
                # Priority fallback: Davis → Ecowitt → Open-Meteo
                my $api_data;
                my $actual_source = 'openmeteo';  # Will track which source succeeded
                
                # Try Davis first (if configured)
                if ($c->config->{davis_api_key}) {
                    $api_data = $self->_fetch_davis_data($c, $location_id, $start_date, $end_date);
                    if ($api_data) {
                        $actual_source = 'davis';
                    }
                }
                
                # Try Ecowitt if Davis failed or not configured
                if (!$api_data && $c->config->{ecowitt_app_key}) {
                    $api_data = $self->_fetch_ecowitt_data($c, $location_id, $start_date, $end_date);
                    if ($api_data) {
                        $actual_source = 'ecowitt';
                    }
                }
                
                # Fall back to Open-Meteo (always available)
                if (!$api_data) {
                    $api_data = $self->_fetch_openmeteo_data($lat, $lon, $start_date, $end_date);
                    $actual_source = 'openmeteo';
                }
                
                $weather_data = $self->_parse_api_response($api_data, $actual_source);
                $data_source = $actual_source;  # Update for response
                
                # Cache the data
                if ($weather_data && scalar(@$weather_data) > 0) {
                    $self->_cache_weather_data($c, $location_id, $weather_data, $data_source);
                    $cache_stats->{from_api} += scalar(@$weather_data);
                }
            }
            
            # Calculate GDD/CHU
            my @daily_data;
            my ($total_gdd, $total_chu, $total_precip) = (0, 0, 0);
            
            foreach my $day (@{$weather_data || []}) {
                my $tmax = $day->{tmax} // 20;
                my $tmin = $day->{tmin} // 10;
                my $precip = $day->{precip} // 0;
                my $tavg = ($tmax + $tmin) / 2;
                
                my $gdd = $tavg > $base_temp ? $tavg - $base_temp : 0;
                $total_gdd += $gdd;
                
                my $chu_max = $tmax > 10 ? 3.33 * ($tmax - 10) - 0.084 * (($tmax - 10) ** 2) : 0;
                my $chu_min = $tmin > 4.4 ? 1.8 * ($tmin - 4.4) : 0;
                # Ontario CHU method: CHU = (CHU_max + CHU_min) / 2
                my $chu_day = ($chu_max + $chu_min) / 2;
                $chu_day = 0 if $chu_day < 0;
                $total_chu += $chu_day;
                $total_precip += $precip;
                
                push @daily_data, {
                    date => $day->{date},
                    temp_max => sprintf("%.1f", $tmax),
                    temp_min => sprintf("%.1f", $tmin),
                    tavg => sprintf("%.1f", $tavg),
                    gdd_day => sprintf("%.1f", $gdd),
                    gdd_cumulative => sprintf("%.1f", $total_gdd),
                    chu_day => sprintf("%.1f", $chu_day),
                    chu_cumulative => sprintf("%.1f", $total_chu),
                    precip_day => sprintf("%.1f", $precip),
                    precip_cumulative => sprintf("%.1f", $total_precip),
                    # Additional weather data
                    humidity_mean => $day->{humidity} ? sprintf("%.0f", $day->{humidity}) : undef,
                    solar_radiation => $day->{solar} ? sprintf("%.2f", $day->{solar}) : undef,
                    evapotranspiration => $day->{et} ? sprintf("%.2f", $day->{et}) : undef,
                    wind_speed_max => $day->{wind} ? sprintf("%.1f", $day->{wind}) : undef,
                    dew_point => $day->{dew_point} ? sprintf("%.1f", $day->{dew_point}) : undef,
                    soil_temp => $day->{soil_temp} ? sprintf("%.1f", $day->{soil_temp}) : undef,
                    soil_moisture => $day->{soil_moisture} ? sprintf("%.3f", $day->{soil_moisture}) : undef,
                    source => $day->{source} || $data_source,
                };
            }
            
            push @results, {
                year => $year,
                start_date => $start_date,
                end_date => $end_date,
                total_gdd => sprintf("%.1f", $total_gdd),
                total_chu => sprintf("%.1f", $total_chu),
                total_precip => sprintf("%.1f", $total_precip),
                days_count => scalar(@daily_data),
                avg_temp => sprintf("%.1f", scalar(@daily_data) > 0 ? ($total_gdd / scalar(@daily_data)) + $base_temp : 0),
                daily_data => \@daily_data,
                data_source => $used_cache ? 'cached' : $data_source,
            };
        }
        
        # Build summary and combine all daily data for Excel export
        my ($sum_gdd, $sum_chu, $sum_precip, $total_days) = (0, 0, 0, 0);
        my @all_daily_data;
        
        foreach my $r (@results) {
            $sum_gdd += $r->{total_gdd};
            $sum_chu += $r->{total_chu};
            $sum_precip += $r->{total_precip};
            $total_days += $r->{days_count};
            # Combine all daily data for Excel export
            push @all_daily_data, @{$r->{daily_data} || []};
        }
        my $years_count = scalar(@results);
        
        $c->stash->{rest} = {
            success => 1,
            multi_year => ($years_count > 1) ? 1 : 0,
            years => \@results,
            # All daily data combined for Excel export
            all_daily_data => \@all_daily_data,
            summary => {
                avg_gdd => sprintf("%.1f", $years_count > 0 ? $sum_gdd / $years_count : 0),
                avg_chu => sprintf("%.1f", $years_count > 0 ? $sum_chu / $years_count : 0),
                avg_precip => sprintf("%.1f", $years_count > 0 ? $sum_precip / $years_count : 0),
                total_days => $total_days,
                years_count => $years_count,
            },
            total_gdd => $results[0]->{total_gdd} || 0,
            total_chu => $results[0]->{total_chu} || 0,
            total_precip => $results[0]->{total_precip} || 0,
            days_count => $results[0]->{days_count} || 0,
            daily_data => $results[0]->{daily_data} || [],
            location => { lat => $lat, lon => $lon },
            sync_info => { 
                synced => $cache_stats->{from_api}, 
                existing => $cache_stats->{from_cache},
                message => "Data from $data_source" 
            },
        };
        
    } catch {
        $c->stash->{rest} = { error => "Failed to calculate GDD: $_" };
    };
}

# ============================================================================
# GDD FOR PERIOD (Phenology Button)
# ============================================================================
# Calculates GDD/CHU between two dates for a location
# Used by "Calculate GDD" button when recording phenology observations

sub gdd_for_period : Path('/ajax/weather/gdd_for_period') Args(0) ActionClass('REST') { }
sub gdd_for_period_GET { shift->_do_gdd_for_period(@_); }
sub gdd_for_period_POST { shift->_do_gdd_for_period(@_); }

sub _do_gdd_for_period {
    my ($self, $c) = @_;
    
    my $location_id = $c->req->param('location_id');
    my $start_date = $c->req->param('start_date');  # Planting date
    my $end_date = $c->req->param('end_date');      # Observation date
    my $base_temp = $c->req->param('base_temp') || 10;
    my $max_temp = $c->req->param('max_temp') || 30;
    
    unless ($location_id && $start_date && $end_date) {
        $c->stash->{rest} = { 
            error => "Missing required parameters: location_id, start_date, end_date" 
        };
        return;
    }
    
    try {
        my ($lat, $lon) = $self->_get_location_coords($c, $location_id);
        
        # Get cached weather data or fetch from API
        my $weather_data = $self->_get_cached_weather($c, $location_id, $start_date, $end_date);
        
        if (!$weather_data || scalar(@$weather_data) == 0) {
            # Fetch from Open-Meteo
            my $api_data = $self->_fetch_openmeteo_data($lat, $lon, $start_date, $end_date);
            $weather_data = $self->_parse_api_response($api_data, 'openmeteo');
            
            if ($weather_data && scalar(@$weather_data) > 0) {
                $self->_cache_weather_data($c, $location_id, $weather_data, 'openmeteo');
            }
        }
        
        # Calculate GDD and CHU
        my ($total_gdd, $total_chu) = (0, 0);
        my $days_count = 0;
        
        foreach my $day (@{$weather_data || []}) {
            my $tmax = $day->{tmax} // 20;
            my $tmin = $day->{tmin} // 10;
            
            # Cap temperatures for GDD calculation
            $tmax = $max_temp if $tmax > $max_temp;
            $tmin = $base_temp if $tmin < $base_temp;
            
            my $tavg = ($tmax + $tmin) / 2;
            my $gdd = $tavg > $base_temp ? $tavg - $base_temp : 0;
            $total_gdd += $gdd;
            
            # CHU calculation (Ontario method)
            my $chu_max = $day->{tmax} > 10 ? 3.33 * ($day->{tmax} - 10) - 0.084 * (($day->{tmax} - 10) ** 2) : 0;
            my $chu_min = $day->{tmin} > 4.4 ? 1.8 * ($day->{tmin} - 4.4) : 0;
            my $chu_day = ($chu_max + $chu_min) / 2;
            $chu_day = 0 if $chu_day < 0;
            $total_chu += $chu_day;
            
            $days_count++;
        }
        
        $c->stash->{rest} = {
            success => 1,
            location_id => $location_id,
            start_date => $start_date,
            end_date => $end_date,
            days_count => $days_count,
            gdd => sprintf("%.1f", $total_gdd),
            chu => sprintf("%.1f", $total_chu),
            base_temp => $base_temp,
            max_temp => $max_temp,
            location => { lat => $lat, lon => $lon },
        };
        
    } catch {
        $c->stash->{rest} = { error => "Failed to calculate GDD for period: $_" };
    };
}

# ============================================================================
# DATABASE CACHING
# ============================================================================

sub _get_cached_weather {
    my ($self, $c, $location_id, $start_date, $end_date) = @_;
    
    my @data;
    try {
        my $dbh = $c->dbc->dbh;
        # Priority merge: Davis/Ecowitt (local station) > Open-Meteo (satellite)
        # DISTINCT ON (date) with ORDER BY priority ensures best source per day
        my $sth = $dbh->prepare(q{
            SELECT DISTINCT ON (date) 
                date, temp_max, temp_min, temp_mean, precipitation, 
                humidity_mean, solar_radiation, evapotranspiration,
                wind_speed_max, dew_point, pressure, uv_index,
                soil_temp, soil_moisture, source
            FROM weather_data
            WHERE location_id = ? AND date BETWEEN ? AND ?
            ORDER BY date,
                CASE source
                    WHEN 'davis' THEN 1
                    WHEN 'ecowitt' THEN 1
                    WHEN 'open-meteo' THEN 2
                    ELSE 3
                END
        });
        $sth->execute($location_id, $start_date, $end_date);
        
        while (my $row = $sth->fetchrow_hashref) {
            push @data, {
                date => $row->{date},
                tmax => $row->{temp_max},
                tmin => $row->{temp_min},
                tavg => $row->{temp_mean},
                precip => $row->{precipitation} || 0,
                humidity => $row->{humidity_mean},
                solar => $row->{solar_radiation},
                et => $row->{evapotranspiration},
                wind => $row->{wind_speed_max},
                dew_point => $row->{dew_point},
                pressure => $row->{pressure},
                uv_index => $row->{uv_index},
                soil_temp => $row->{soil_temp},
                soil_moisture => $row->{soil_moisture},
                source => $row->{source},
            };
        }
    } catch {
        warn "Failed to get cached weather: $_";
    };
    
    return \@data;
}

sub _cache_weather_data {
    my ($self, $c, $location_id, $data, $source) = @_;
    
    try {
        my $dbh = $c->dbc->dbh;
        
        # Use weather_data table (supports multiple sources per date)
        # Save all available fields from Open-Meteo and other sources
        my $sth = $dbh->prepare(q{
            INSERT INTO weather_data 
                (location_id, date, temp_max, temp_min, temp_mean, 
                 precipitation, humidity_mean, solar_radiation, 
                 evapotranspiration, wind_speed_max, dew_point,
                 soil_temp, soil_moisture, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (location_id, date, source) DO UPDATE SET
                temp_max = EXCLUDED.temp_max,
                temp_min = EXCLUDED.temp_min,
                temp_mean = EXCLUDED.temp_mean,
                precipitation = EXCLUDED.precipitation,
                humidity_mean = EXCLUDED.humidity_mean,
                solar_radiation = EXCLUDED.solar_radiation,
                evapotranspiration = EXCLUDED.evapotranspiration,
                wind_speed_max = EXCLUDED.wind_speed_max,
                dew_point = EXCLUDED.dew_point,
                soil_temp = EXCLUDED.soil_temp,
                soil_moisture = EXCLUDED.soil_moisture
        });
        
        foreach my $day (@$data) {
            # Use tavg from API if available, else calculate
            my $tavg = $day->{tavg} // (
                defined($day->{tmax}) && defined($day->{tmin}) 
                    ? ($day->{tmax} + $day->{tmin}) / 2 
                    : undef
            );
            $sth->execute(
                $location_id,
                $day->{date},
                $day->{tmax},
                $day->{tmin},
                $tavg,
                $day->{precip} || 0,
                $day->{humidity},
                $day->{solar},
                $day->{et},
                $day->{wind},
                $day->{dew_point},
                $day->{soil_temp},
                $day->{soil_moisture},
                $source eq 'openmeteo' ? 'open-meteo' : $source
            );
        }
        
        warn "Cached " . scalar(@$data) . " weather records for location $location_id from $source";
    } catch {
        warn "Failed to cache weather data: $_";
    };
}

# ============================================================================
# OPEN-METEO API (Free, no API key)
# ============================================================================

sub _fetch_openmeteo_data {
    my ($self, $lat, $lon, $start_date, $end_date) = @_;
    
    my $ua = LWP::UserAgent->new(timeout => 60);
    
    # Fetch ALL available daily variables from Open-Meteo Historical API
    my @daily_vars = qw(
        temperature_2m_max temperature_2m_min temperature_2m_mean
        precipitation_sum rain_sum snowfall_sum precipitation_hours
        sunshine_duration et0_fao_evapotranspiration
        wind_speed_10m_max wind_gusts_10m_max wind_direction_10m_dominant
        shortwave_radiation_sum relative_humidity_2m_mean dew_point_2m_mean
        soil_temperature_0_to_7cm_mean soil_moisture_0_to_7cm_mean
    );
    my $daily_params = join(',', @daily_vars);
    
    my $url = sprintf(
        "https://archive-api.open-meteo.com/v1/archive?latitude=%.4f&longitude=%.4f&start_date=%s&end_date=%s&daily=%s&timezone=auto",
        $lat, $lon, $start_date, $end_date, $daily_params
    );
    
    my $response = $ua->get($url);
    return $response->is_success ? decode_json($response->decoded_content) : undef;
}

# ============================================================================
# DAVIS WEATHERLINK v2 API
# ============================================================================

sub _fetch_davis_data {
    my ($self, $c, $location_id, $start_date, $end_date) = @_;
    
    my $api_key = $c->config->{davis_api_key} || '';
    my $api_secret = $c->config->{davis_api_secret} || '';
    my $station_id = $c->config->{davis_station_id} || '';
    
    return undef unless $api_key && $api_secret && $station_id;
    
    my $ua = LWP::UserAgent->new(timeout => 30);
    
    # Convert dates to Unix timestamps
    my $start_ts = $self->_date_to_timestamp($start_date);
    my $end_ts = $self->_date_to_timestamp($end_date);
    
    # Build API request with HMAC signature
    my $t = time();
    my $params = "api-key=$api_key&end-timestamp=$end_ts&start-timestamp=$start_ts&station-id=$station_id&t=$t";
    my $signature = hmac_sha256_hex($params, $api_secret);
    
    my $url = "https://api.weatherlink.com/v2/historic/$station_id?$params";
    
    my $response = $ua->get($url, 
        'X-Api-Secret' => $signature
    );
    
    if ($response->is_success) {
        return decode_json($response->decoded_content);
    } else {
        warn "Davis API error: " . $response->status_line;
        return undef;
    }
}

# ============================================================================
# ECOWITT CLOUD API
# ============================================================================

sub _fetch_ecowitt_data {
    my ($self, $c, $location_id, $start_date, $end_date) = @_;
    
    my $app_key = $c->config->{ecowitt_app_key} || '';
    my $api_key = $c->config->{ecowitt_api_key} || '';
    my $mac = $c->config->{ecowitt_mac} || '';
    
    return undef unless $app_key && $api_key && $mac;
    
    my $ua = LWP::UserAgent->new(timeout => 30);
    
    my $url = "https://api.ecowitt.net/api/v3/device/history";
    my $response = $ua->post($url, {
        application_key => $app_key,
        api_key => $api_key,
        mac => $mac,
        start_date => $start_date,
        end_date => $end_date,
        call_back => 'outdoor,rainfall',
        cycle_type => 'day',
    });
    
    if ($response->is_success) {
        return decode_json($response->decoded_content);
    } else {
        warn "Ecowitt API error: " . $response->status_line;
        return undef;
    }
}

# ============================================================================
# API RESPONSE PARSING
# ============================================================================

sub _parse_api_response {
    my ($self, $data, $source) = @_;
    
    return [] unless $data;
    
    my @result;
    
    if ($source eq 'openmeteo' || $source eq 'open-meteo') {
        my $daily = $data->{daily} || {};
        my $dates = $daily->{time} || [];
        
        for my $i (0..$#$dates) {
            push @result, {
                date       => $dates->[$i],
                tmax       => $daily->{temperature_2m_max}[$i],
                tmin       => $daily->{temperature_2m_min}[$i],
                tavg       => $daily->{temperature_2m_mean}[$i],
                precip     => $daily->{precipitation_sum}[$i] // 0,
                rain       => $daily->{rain_sum}[$i],
                snowfall   => $daily->{snowfall_sum}[$i],
                precip_hours => $daily->{precipitation_hours}[$i],
                sunshine   => $daily->{sunshine_duration}[$i],  # seconds
                et         => $daily->{et0_fao_evapotranspiration}[$i],
                wind       => $daily->{wind_speed_10m_max}[$i],
                wind_gusts => $daily->{wind_gusts_10m_max}[$i],
                wind_dir   => $daily->{wind_direction_10m_dominant}[$i],
                solar      => $daily->{shortwave_radiation_sum}[$i],  # MJ/m²
                humidity   => $daily->{relative_humidity_2m_mean}[$i],
                dew_point  => $daily->{dew_point_2m_mean}[$i],
                soil_temp  => $daily->{soil_temperature_0_to_7cm_mean}[$i],
                soil_moisture => $daily->{soil_moisture_0_to_7cm_mean}[$i],
            };
        }
    }
    elsif ($source eq 'davis') {
        my $sensors = $data->{sensors} || [];
        foreach my $sensor (@$sensors) {
            next unless $sensor->{sensor_type} == 45; # ISS sensor
            foreach my $rec (@{$sensor->{data} || []}) {
                push @result, {
                    date => $self->_timestamp_to_date($rec->{ts}),
                    tmax => $rec->{temp_hi_at} ? ($rec->{temp_hi_at} - 32) * 5/9 : undef,
                    tmin => $rec->{temp_lo_at} ? ($rec->{temp_lo_at} - 32) * 5/9 : undef,
                    precip => $rec->{rainfall_mm} // 0,
                };
            }
        }
    }
    elsif ($source eq 'ecowitt') {
        my $outdoor = $data->{data}{outdoor} || {};
        my $temps = $outdoor->{temperature}{list} || [];
        my $rain = $data->{data}{rainfall}{daily}{list} || [];
        
        for my $i (0..$#$temps) {
            my $t = $temps->[$i];
            push @result, {
                date => $t->{time},
                tmax => $t->{high},
                tmin => $t->{low},
                precip => $rain->[$i]{value} // 0,
            };
        }
    }
    
    return \@result;
}

# ============================================================================
# STATION CONFIGURATION
# ============================================================================

sub get_station_config : Path('/ajax/weather/station/config') Args(0) ActionClass('REST') { }
sub get_station_config_GET {
    my ($self, $c) = @_;
    
    my $config = {
        source => 'openmeteo',
        davis_configured => ($c->config->{davis_api_key} ? 1 : 0),
        ecowitt_configured => ($c->config->{ecowitt_app_key} ? 1 : 0),
        description => 'Weather data from Open-Meteo Historical API',
    };
    
    $c->stash->{rest} = { success => 1, config => $config };
}

sub save_station_config : Path('/ajax/weather/station/config') Args(0) ActionClass('REST') { }
sub save_station_config_POST {
    my ($self, $c) = @_;
    # Note: Config changes would need to be persisted to sgn_local.conf
    $c->stash->{rest} = { success => 1, message => "Configuration updated" };
}

# ============================================================================
# DATA SOURCES LIST
# ============================================================================

sub get_data_sources : Path('/ajax/weather/sources') Args(0) ActionClass('REST') { }
sub get_data_sources_GET {
    my ($self, $c) = @_;
    
    my @sources = (
        { 
            id => 'openmeteo', 
            name => 'Open-Meteo', 
            description => 'Free historical weather API (1940-present)',
            configured => 1,
            requires_key => 0,
        },
        { 
            id => 'davis', 
            name => 'Davis WeatherLink', 
            description => 'Davis Instruments weather stations',
            configured => ($c->config->{davis_api_key} ? 1 : 0),
            requires_key => 1,
        },
        { 
            id => 'ecowitt', 
            name => 'Ecowitt Cloud', 
            description => 'Ecowitt weather stations',
            configured => ($c->config->{ecowitt_app_key} ? 1 : 0),
            requires_key => 1,
        },
    );
    
    $c->stash->{rest} = { success => 1, sources => \@sources };
}

# ============================================================================
# CACHE STATISTICS
# ============================================================================

sub get_cache_stats : Path('/ajax/weather/cache/stats') Args(0) ActionClass('REST') { }
sub get_cache_stats_GET {
    my ($self, $c) = @_;
    
    my $stats = { total_records => 0, locations => 0, date_range => {} };
    
    try {
        my $dbh = $c->dbc->dbh;
        my $sth = $dbh->prepare(q{
            SELECT COUNT(*) as total, 
                   COUNT(DISTINCT location_id) as locations,
                   MIN(date) as min_date,
                   MAX(date) as max_date
            FROM weather_data
        });
        $sth->execute();
        my $row = $sth->fetchrow_hashref;
        
        $stats = {
            total_records => $row->{total} || 0,
            locations => $row->{locations} || 0,
            date_range => {
                min => $row->{min_date} || 'N/A',
                max => $row->{max_date} || 'N/A',
            },
        };
    } catch {
        # Table doesn't exist yet
    };
    
    $c->stash->{rest} = { success => 1, stats => $stats };
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

sub _get_location_coords {
    my ($self, $c, $location_id) = @_;
    
    my ($lat, $lon) = (49.97, 33.60);  # Default: Mirgorod, Ukraine
    
    if ($location_id) {
        try {
            my $schema = $c->dbic_schema('Bio::Chado::Schema', 'sgn_chado');
            my $loc = $schema->resultset('NaturalDiversity::NdGeolocation')->find($location_id);
            if ($loc) {
                $lat = $loc->latitude if defined $loc->latitude;
                $lon = $loc->longitude if defined $loc->longitude;
            }
        } catch { };
    }
    
    return ($lat, $lon);
}

sub _date_to_timestamp {
    my ($self, $date) = @_;
    my ($y, $m, $d) = split /-/, $date;
    use Time::Local;
    return timelocal(0, 0, 0, $d, $m - 1, $y);
}

sub _timestamp_to_date {
    my ($self, $ts) = @_;
    my @t = localtime($ts);
    return sprintf("%04d-%02d-%02d", $t[5] + 1900, $t[4] + 1, $t[3]);
}

# ============================================================================
# STORE GDD/CHU PHENOTYPES (Maturity Calculator)
# ============================================================================

sub store_gdd_phenotypes : Path('/ajax/phenotype/store_gdd_batch') Args(0) ActionClass('REST') { }
sub store_gdd_phenotypes_POST {
    my ($self, $c) = @_;
    
    my $trial_id = $c->req->param('trial_id');
    my $accession_ids_json = $c->req->param('accession_ids');
    my $gdd_value = $c->req->param('gdd');
    my $chu_value = $c->req->param('chu');
    my $start_date = $c->req->param('start_date');
    my $end_date = $c->req->param('end_date');
    my $location_id = $c->req->param('location_id');
    
    unless ($trial_id && $accession_ids_json && $gdd_value && $chu_value) {
        $c->stash->{rest} = { error => "Missing required parameters" };
        return;
    }
    
    try {
        my $accession_ids = decode_json($accession_ids_json);
        my $dbh = $c->dbc->dbh;
        
        # Get GDD and CHU trait IDs
        my $sth_trait = $dbh->prepare(q{
            SELECT cvterm_id, name FROM cvterm 
            WHERE name IN ('Growing Degree Days', 'Crop Heat Units')
            AND cv_id = (SELECT cv_id FROM cv WHERE name = 'maize_trait')
        });
        $sth_trait->execute();
        
        my %trait_ids;
        while (my $row = $sth_trait->fetchrow_hashref) {
            $trait_ids{$row->{name}} = $row->{cvterm_id};
        }
        
        my $gdd_trait_id = $trait_ids{'Growing Degree Days'};
        my $chu_trait_id = $trait_ids{'Crop Heat Units'};
        
        unless ($gdd_trait_id && $chu_trait_id) {
            $c->stash->{rest} = { error => "GDD/CHU traits not found in maize ontology" };
            return;
        }
        
        # Get experiment/plot IDs for the accessions in this trial
        my $placeholders = join(',', ('?') x scalar(@$accession_ids));
        my $sth_plots = $dbh->prepare(qq{
            SELECT s.stock_id, nde.nd_experiment_id
            FROM stock s
            JOIN nd_experiment_stock nes ON s.stock_id = nes.stock_id
            JOIN nd_experiment nde ON nes.nd_experiment_id = nde.nd_experiment_id
            JOIN nd_experiment_project ndep ON nde.nd_experiment_id = ndep.nd_experiment_id
            WHERE ndep.project_id = ?
            AND s.stock_id IN ($placeholders)
        });
        $sth_plots->execute($trial_id, @$accession_ids);
        
        my %stock_experiments;
        while (my $row = $sth_plots->fetchrow_hashref) {
            $stock_experiments{$row->{stock_id}} = $row->{nd_experiment_id};
        }
        
        # Insert phenotypes
        my $sth_insert = $dbh->prepare(q{
            INSERT INTO phenotype (observable_id, value, nd_experiment_id, cvalue_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT DO NOTHING
        });
        
        my $created = 0;
        foreach my $stock_id (@$accession_ids) {
            my $exp_id = $stock_experiments{$stock_id};
            next unless $exp_id;
            
            # Insert GDD phenotype
            $sth_insert->execute($gdd_trait_id, $gdd_value, $exp_id, undef);
            $created++ if $sth_insert->rows > 0;
            
            # Insert CHU phenotype
            $sth_insert->execute($chu_trait_id, $chu_value, $exp_id, undef);
            $created++ if $sth_insert->rows > 0;
        }
        
        $c->stash->{rest} = {
            success => 1,
            created => $created,
            message => "Created $created GDD/CHU phenotypes"
        };
        
    } catch {
        $c->stash->{rest} = { error => "Failed to store phenotypes: $_" };
    };
}

1;
