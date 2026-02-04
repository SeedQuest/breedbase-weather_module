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
# MATURITY TRAITS DISCOVERY ENDPOINT (for portable deployment)
# ============================================================================

sub get_maturity_traits : Path('/ajax/weather/maturity_traits') Args(0) ActionClass('REST') { }
sub get_maturity_traits_GET {
    my ($self, $c) = @_;
    
    my $crop = $c->req->param('crop') || 'maize_trait';
    
    my $dbh = $c->dbc->dbh();
    
    # Search for maturity-related traits in the specified ontology
    my $sth = $dbh->prepare(q{
        SELECT c.cvterm_id, c.name, cv.name as ontology
        FROM cvterm c
        JOIN cv ON c.cv_id = cv.cv_id
        WHERE cv.name = ?
        AND (
            c.name ILIKE '%maturity time%'
            OR c.name ILIKE '%days to maturity%'
            OR c.name ILIKE '%maturity group%'
        )
        ORDER BY c.name
    });
    $sth->execute($crop);
    
    my @traits;
    while (my $row = $sth->fetchrow_hashref) {
        my $name = $row->{name};
        my $format = 'unknown';
        
        # Detect format from trait name
        if ($name =~ /-(?: |)day(?:s)?$/i || $name =~ /time - day/i) {
            $format = 'day';
        } elsif ($name =~ /yyyymmdd/i || $name =~ /date/i) {
            $format = 'date';
        }
        
        push @traits, {
            id       => $row->{cvterm_id},
            name     => $name,
            ontology => $row->{ontology},
            format   => $format,
        };
    }
    
    # Sort: supported formats first (day, date), then unknown
    @traits = sort { 
        ($a->{format} eq 'unknown' ? 1 : 0) <=> ($b->{format} eq 'unknown' ? 1 : 0) 
        || $a->{name} cmp $b->{name}
    } @traits;
    
    $c->stash->{rest} = { 
        success => 1,
        crop    => $crop,
        traits  => \@traits,
        count   => scalar(@traits)
    };
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
        my $seasons;
        if ($seasons_json) {
            $seasons = decode_json($seasons_json);
        } else {
            # Fallback: construct season from start_date/end_date params
            my $start = $c->req->param('start_date');
            my $end = $c->req->param('end_date');
            if ($start && $end) {
                my ($year) = $start =~ /^(\d{4})/;
                $seasons = [{ year => $year, start_date => $start, end_date => $end }];
            } else {
                die "Missing required parameters: seasons or start_date/end_date";
            }
        }
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
# GET TRIAL LOCATION ID
# ============================================================================

sub get_trial_location_id : Path('/ajax/trial') Args(2) ActionClass('REST') {
    my ($self, $c, $trial_id, $action) = @_;
    return unless $action eq 'location_id';
}

sub get_trial_location_id_GET {
    my ($self, $c, $trial_id, $action) = @_;
    
    my $dbh = $c->dbc->dbh;
    
    # Get location_id from projectprop
    my $sth = $dbh->prepare(q{
        SELECT pp.value::integer as location_id, g.description as location_name
        FROM projectprop pp
        JOIN cvterm cv ON pp.type_id = cv.cvterm_id
        LEFT JOIN nd_geolocation g ON pp.value::integer = g.nd_geolocation_id
        WHERE pp.project_id = ? AND cv.name = 'project location'
        LIMIT 1
    });
    $sth->execute($trial_id);
    
    my $row = $sth->fetchrow_hashref;
    
    $c->stash->{rest} = {
        location_id => $row ? $row->{location_id} : undef,
        location_name => $row ? $row->{location_name} : undef,
        trial_id => $trial_id
    };
}

# ============================================================================
# GET TRIAL PHENOLOGY DATA (Per-Accession Dates)
# ============================================================================

sub get_trial_phenology : Path('/ajax/trial') Args(2) ActionClass('REST') {
    my ($self, $c, $trial_id, $action) = @_;
    return unless $action eq 'phenology';
}

sub get_trial_phenology_GET {
    my ($self, $c, $trial_id, $action) = @_;
    
    my $dbh = $c->dbc->dbh;
    
    # Crop-specific trait IDs
    my %crop_traits = (
        'maize_trait' => {  # Maize
            emergence => 97939,
            flowering => [81054, 81052],  # Silking time - day, Anthesis silking interval
            maturity  => [80767, 81183, 97771],  # Maturity time - day variants
            gdd => 97927, chu => 97929, fao => 97931
        },
        'CO_336' => {  # Soybean
            emergence => 97940,
            flowering => [81093, 81215],  # First flower, Flowering time
            maturity  => [81098, 81084],  # Maturity time, R8
            gdd => 97932, chu => 97933, fao => 97934
        },
        'CO_359' => {  # Sunflower
            emergence => [81733, 81669],  # Cotyledon, Seedling
            flowering => 81609,
            maturity  => [81736, 81681],
            gdd => 97935, chu => 97936, fao => 97937
        },
        'CO_358' => {  # Cotton
            emergence => 82920,  # Days to 50% emergence
            flowering => 82733,  # Days to 50% flowering
            maturity  => 82577,  # Days to 50% boll opening
            gdd => 97942, chu => 97943, fao => 97944
        }
    );
    
    # Get trial dates
    my $sth_trial = $dbh->prepare(q{
        SELECT 
            (SELECT value FROM projectprop WHERE project_id = p.project_id AND type_id = (SELECT cvterm_id FROM cvterm WHERE name = 'project_planting_date')) as planting_date,
            (SELECT value FROM projectprop WHERE project_id = p.project_id AND type_id = (SELECT cvterm_id FROM cvterm WHERE name = 'project_harvest_date')) as harvest_date,
            (SELECT value::integer FROM projectprop WHERE project_id = p.project_id AND type_id = (SELECT cvterm_id FROM cvterm WHERE name = 'project location')) as location_id
        FROM project p
        WHERE p.project_id = ?
    });
    $sth_trial->execute($trial_id);
    my $trial_row = $sth_trial->fetchrow_hashref;
    
    # Parse JSON date format: {"2025-04-15T00:00:00",...}
    my $planting = $trial_row->{planting_date} || '';
    my $harvest = $trial_row->{harvest_date} || '';
    my $location_id = $trial_row->{location_id};
    $planting =~ s/.*?(\d{4}-\d{2}-\d{2}).*/$1/ if $planting;
    $harvest =~ s/.*?(\d{4}-\d{2}-\d{2}).*/$1/ if $harvest;
    
    # Detect crop type from trial
    my $sth_crop = $dbh->prepare(q{
        SELECT DISTINCT cv.name as cv_name
        FROM project p
        JOIN nd_experiment_project nep ON p.project_id = nep.project_id
        JOIN nd_experiment_stock nes ON nep.nd_experiment_id = nes.nd_experiment_id
        JOIN stock st ON nes.stock_id = st.stock_id  
        JOIN cvterm c ON st.type_id = c.cvterm_id
        JOIN stock_cvterm sc ON st.stock_id = sc.stock_id
        JOIN cvterm trait ON sc.cvterm_id = trait.cvterm_id
        JOIN cv ON trait.cv_id = cv.cv_id
        WHERE p.project_id = ? AND cv.name IN ('maize_trait', 'CO_336', 'CO_359', 'CO_358')
        LIMIT 1
    });
    $sth_crop->execute($trial_id);
    my ($detected_cv) = $sth_crop->fetchrow_array;
    $detected_cv ||= 'maize_trait';  # Default to maize
    
    my $traits = $crop_traits{$detected_cv};
    
    # Build trait ID list for lookup
    my @trait_ids;
    foreach my $type (qw(emergence flowering maturity)) {
        my $t = $traits->{$type};
        if (ref($t) eq 'ARRAY') {
            push @trait_ids, @$t;
        } elsif ($t) {
            push @trait_ids, $t;
        }
    }
    my $trait_list = join(',', @trait_ids) || '0';
    
    # Step 1: Get stocks (plots) from this trial via project
    my $sth_stocks = $dbh->prepare(q{
        SELECT DISTINCT s.stock_id, s.uniquename as plot_name
        FROM stock s
        JOIN nd_experiment_stock nes ON s.stock_id = nes.stock_id
        JOIN nd_experiment_project nep ON nes.nd_experiment_id = nep.nd_experiment_id
        WHERE nep.project_id = ?
        ORDER BY s.uniquename
    });
    $sth_stocks->execute($trial_id);
    
    my %stock_ids;
    while (my $row = $sth_stocks->fetchrow_hashref) {
        $stock_ids{$row->{stock_id}} = $row->{plot_name};
    }
    
    return $c->stash->{rest} = { error => "No stocks in trial" } unless %stock_ids;
    
    my $stock_list = join(',', keys %stock_ids);
    
    # Step 2: Get phenotypes for these stocks via nd_experiment_stock -> nd_experiment_phenotype
    my $sql = qq{
        SELECT s.stock_id, s.uniquename as plot_name,
               p.observable_id as trait_id, p.value as trait_value, p.collect_date
        FROM stock s
        JOIN nd_experiment_stock nes ON s.stock_id = nes.stock_id
        JOIN nd_experiment_phenotype nep ON nes.nd_experiment_id = nep.nd_experiment_id
        JOIN phenotype p ON nep.phenotype_id = p.phenotype_id
        WHERE s.stock_id IN ($stock_list)
        AND p.observable_id IN ($trait_list)
    };
    my $pheno_rows_ref = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    # Initialize all plots with defaults
    my %plot_data;
    foreach my $sid (keys %stock_ids) {
        $plot_data{$sid} = {
            stock_id => $sid,
            name => $stock_ids{$sid},
            has_emergence => 0,
            has_flowering => 0,
            has_maturity => 0,
            emergence_value => undef,
            flowering_value => undef,
            maturity_value => undef,
        };
    }
    
    # Process phenotype data for plots
    my $pheno_rows = scalar @$pheno_rows_ref;
    foreach my $row (@$pheno_rows_ref) {
        my $sid = $row->{stock_id};
        next unless $plot_data{$sid};
        
        my $tid = $row->{trait_id};
        next unless $tid;
        
        my $emerg_ids = ref($traits->{emergence}) eq 'ARRAY' ? $traits->{emergence} : [$traits->{emergence}];
        my $flower_ids = ref($traits->{flowering}) eq 'ARRAY' ? $traits->{flowering} : [$traits->{flowering}];
        my $matur_ids = ref($traits->{maturity}) eq 'ARRAY' ? $traits->{maturity} : [$traits->{maturity}];
        
        if (grep { $_ == $tid } @$emerg_ids) {
            $plot_data{$sid}{has_emergence} = 1;
            $plot_data{$sid}{emergence_value} = $row->{trait_value};
        } elsif (grep { $_ == $tid } @$flower_ids) {
            $plot_data{$sid}{has_flowering} = 1;
            $plot_data{$sid}{flowering_value} = $row->{trait_value};
        } elsif (grep { $_ == $tid } @$matur_ids) {
            $plot_data{$sid}{has_maturity} = 1;
            $plot_data{$sid}{maturity_value} = $row->{trait_value};
        }
    }
    
    # Group by germplasm (extract from plot name: lub-trial-repX-GERMPLASM_N)
    my %germplasm_data;
    foreach my $sid (keys %plot_data) {
        my $plot_name = $plot_data{$sid}{name};
        my $germplasm;
        
        # Extract germplasm: try pattern "repX-NAME_N" or just use plot name
        if ($plot_name =~ /rep\d+-([^_]+)/) {
            $germplasm = $1;
        } else {
            $germplasm = $plot_name;
        }
        
        if (!$germplasm_data{$germplasm}) {
            $germplasm_data{$germplasm} = {
                name => $germplasm,
                plot_count => 0,
                plots_with_emergence => 0,
                plots_with_flowering => 0,
                plots_with_maturity => 0,
                avg_emergence => 0,
                avg_flowering => 0,
                avg_maturity => 0,
                emergence_values => [],
                flowering_values => [],
                maturity_values => [],
                plot_ids => [],
            };
        }
        
        my $g = $germplasm_data{$germplasm};
        $g->{plot_count}++;
        push @{$g->{plot_ids}}, $sid;
        
        if ($plot_data{$sid}{has_emergence}) {
            $g->{plots_with_emergence}++;
            push @{$g->{emergence_values}}, $plot_data{$sid}{emergence_value};
        }
        if ($plot_data{$sid}{has_flowering}) {
            $g->{plots_with_flowering}++;
            push @{$g->{flowering_values}}, $plot_data{$sid}{flowering_value};
        }
        if ($plot_data{$sid}{has_maturity}) {
            $g->{plots_with_maturity}++;
            push @{$g->{maturity_values}}, $plot_data{$sid}{maturity_value};
        }
    }
    
    # Calculate averages and determine validation status
    my @accessions;
    foreach my $germ (sort keys %germplasm_data) {
        my $g = $germplasm_data{$germ};
        
        # Calculate averages
        if (@{$g->{emergence_values}}) {
            my $sum = 0; $sum += $_ for @{$g->{emergence_values}};
            $g->{avg_emergence} = sprintf("%.1f", $sum / scalar(@{$g->{emergence_values}}));
        }
        if (@{$g->{flowering_values}}) {
            my $sum = 0; $sum += $_ for @{$g->{flowering_values}};
            $g->{avg_flowering} = sprintf("%.1f", $sum / scalar(@{$g->{flowering_values}}));
        }
        if (@{$g->{maturity_values}}) {
            my $sum = 0; $sum += $_ for @{$g->{maturity_values}};
            $g->{avg_maturity} = sprintf("%.1f", $sum / scalar(@{$g->{maturity_values}}));
        }
        
        # Determine what's missing
        my @missing;
        push @missing, 'emergence' unless $g->{plots_with_emergence};
        push @missing, 'flowering' unless $g->{plots_with_flowering};
        push @missing, 'maturity' unless $g->{plots_with_maturity};
        
        # Validation status: ready if has at least maturity OR flowering
        my $is_ready = ($g->{plots_with_maturity} > 0 || $g->{plots_with_flowering} > 0) ? 1 : 0;
        
        push @accessions, {
            name => $germ,
            plot_count => $g->{plot_count},
            plot_ids => $g->{plot_ids},
            has_emergence => $g->{plots_with_emergence} > 0 ? 1 : 0,
            has_flowering => $g->{plots_with_flowering} > 0 ? 1 : 0,
            has_maturity => $g->{plots_with_maturity} > 0 ? 1 : 0,
            emergence_value => $g->{avg_emergence} || undef,
            flowering_value => $g->{avg_flowering} || undef,
            maturity_value => $g->{avg_maturity} || undef,
            missing => \@missing,
            is_ready => $is_ready,
        };
    }
    
    # Count ready/not ready
    my $ready_count = grep { $_->{is_ready} } @accessions;
    my $not_ready_count = scalar(@accessions) - $ready_count;
    
    $c->stash->{rest} = {
        trial => {
            id => $trial_id,
            planting_date => $planting,
            harvest_date => $harvest,
            location_id => $location_id,
        },
        crop => $detected_cv,
        traits => $traits,
        summary => {
            total_germplasm => scalar(@accessions),
            ready => $ready_count,
            not_ready => $not_ready_count,
            total_plots => scalar(keys %stock_ids),
        },
        accessions => \@accessions
    };
}

# ============================================================================
# GET PHENOLOGY TRAITS (Dynamic Ontology Loading)
# ============================================================================

sub get_phenology_traits : Path('/ajax/phenology/traits') Args(0) ActionClass('REST') { }
sub get_phenology_traits_GET {
    my ($self, $c) = @_;
    
    my $dbh = $c->dbc->dbh;
    my $crop = $c->req->param('crop') || 'maize';
    
    # Search for relevant traits by keyword patterns
    my $sth = $dbh->prepare(q{
        SELECT cv.cvterm_id, cv.name, db.name as ontology
        FROM cvterm cv
        JOIN dbxref dx ON cv.dbxref_id = dx.dbxref_id
        JOIN db ON dx.db_id = db.db_id
        WHERE (
            LOWER(cv.name) LIKE '%emergence%' OR
            LOWER(cv.name) LIKE '%seedling%' OR
            LOWER(cv.name) LIKE '%flowering%' OR
            LOWER(cv.name) LIKE '%silking%' OR
            LOWER(cv.name) LIKE '%anthesis%' OR
            LOWER(cv.name) LIKE '%maturity%' OR
            LOWER(cv.name) LIKE '%black layer%'
        )
        AND db.name IN ('CO_322', 'CO_336', 'CO_359', 'CO_317', 'maize_trait', 'soybean_trait', 'sunflower_trait', 'cotton_trait')
        ORDER BY cv.name
        LIMIT 100
    });
    $sth->execute();
    
    my @emergence; my @flowering; my @maturity;
    while (my $row = $sth->fetchrow_hashref) {
        my $lower = lc($row->{name});
        my $trait = { id => $row->{cvterm_id}, name => $row->{name}, ontology => $row->{ontology} };
        
        if ($lower =~ /emergence|seedling/) {
            push @emergence, $trait;
        } elsif ($lower =~ /flowering|silking|anthesis/) {
            push @flowering, $trait;
        } elsif ($lower =~ /maturity|black layer/) {
            push @maturity, $trait;
        }
    }
    
    # Get defaults for current crop
    my %defaults = (
        maize => { emergence => 97939, flowering => 81054, maturity => 80767 },
        soybean => { emergence => 97940, flowering => 81093, maturity => 81098 },
        sunflower => { emergence => 81733, flowering => 81729, maturity => 81722 },
        cotton => { emergence => 81596, flowering => 81563, maturity => 81535 },
    );
    
    $c->stash->{rest} = {
        traits => {
            emergence => \@emergence,
            flowering => \@flowering,
            maturity => \@maturity,
        },
        defaults => $defaults{$crop} || $defaults{maize},
        crop => $crop,
    };
}

# ============================================================================
# STORE GDD/CHU PHENOTYPES (Maturity Calculator)
# ============================================================================

sub store_gdd_phenotypes : Path('/ajax/phenotype/store_gdd_batch') Args(0) ActionClass('REST') { }
sub store_gdd_phenotypes_POST {
    my ($self, $c) = @_;
    
    my $trial_id = $c->req->param('trial_id');
    my $accession_ids_json = $c->req->param('accession_ids');
    my $crop = $c->req->param('crop') || 'maize_trait';
    # User-selectable maturity trait - if provided, use it; otherwise use defaults
    my $user_maturity_trait_id = $c->req->param('maturity_trait_id');
    my $user_emergence_trait_id = $c->req->param('emergence_trait_id');
    my $use_planting_fallback = $c->req->param('use_planting_fallback') || 0;
    my $fallback_emergence_days = $c->req->param('fallback_emergence_days') || 7;
    # GDD/CHU from frontend are now optional - we calculate per-plot
    my $fallback_gdd = $c->req->param('gdd') || 0;
    my $fallback_chu = $c->req->param('chu') || 0;
    
    unless ($trial_id && $accession_ids_json) {
        $c->stash->{rest} = { error => "Missing required parameters" };
        return;
    }
    
    # Crop-specific trait IDs for OUTPUT (GDD/CHU phenotypes)
    my %crop_gdd_traits = (
        'maize_trait' => 97927,
        'CO_336'      => 97932,
        'CO_359'      => 97935,
        'CO_358'      => 97942,
    );
    my %crop_chu_traits = (
        'maize_trait' => 97929,
        'CO_336'      => 97933,
        'CO_359'      => 97936,
        'CO_358'      => 97943,
    );
    
    my $gdd_trait_id = $crop_gdd_traits{$crop};
    my $chu_trait_id = $crop_chu_traits{$crop};
    
    # Default maturity traits (fallback if user doesn't specify)
    my %default_maturity_day = (
        'maize_trait' => 80767,
        'CO_336'      => 80767,
        'CO_359'      => 80767,
        'CO_358'      => 80767,
    );
    my %default_maturity_date = (
        'maize_trait' => 80820,
        'CO_336'      => 80820,
        'CO_359'      => 80820,
        'CO_358'      => 80820,
    );
    
    # Default emergence traits
    my %default_emergence = (
        'maize_trait' => 97939,  # Seedling emergence - Date (yymmdd)
        'CO_336'      => 97941,
        'CO_359'      => 97939,
        'CO_358'      => 97939,
    );
    
    my $maturity_day_trait_id;
    my $maturity_date_trait_id;
    my $emergence_trait_id;
    my $user_trait_format = 'unknown';
    
    if ($user_maturity_trait_id) {
        # User selected specific trait - determine its format
        my $dbh = $c->dbc->dbh();
        my $sth = $dbh->prepare("SELECT name FROM cvterm WHERE cvterm_id = ?");
        $sth->execute($user_maturity_trait_id);
        my ($trait_name) = $sth->fetchrow_array();
        
        if (!$trait_name) {
            $c->stash->{rest} = { error => "Selected maturity trait (ID: $user_maturity_trait_id) not found in database" };
            return;
        }
        
        # User selected a specific trait - we'll use it and detect format from actual values
        # Set it as both day and date trait - format determined later from actual data
        $maturity_day_trait_id = $user_maturity_trait_id;
        $maturity_date_trait_id = $user_maturity_trait_id;
        $user_trait_format = 'auto';  # Will be determined from actual data values
    } else {
        # Use defaults for this crop
        $maturity_day_trait_id = $default_maturity_day{$crop};
        $maturity_date_trait_id = $default_maturity_date{$crop};
    }
    
    # Set emergence trait
    if ($user_emergence_trait_id) {
        $emergence_trait_id = $user_emergence_trait_id;
    } else {
        $emergence_trait_id = $default_emergence{$crop};
    }
    
    unless ($gdd_trait_id && $chu_trait_id) {
        $c->stash->{rest} = { error => "Unknown crop type: $crop" };
        return;
    }
    
    try {
        my $accession_ids = decode_json($accession_ids_json);
        my $dbh = $c->dbc->dbh;
        
        # Get trial info: planting date and location
        my $sth_trial = $dbh->prepare(q{
            SELECT 
                projectprop_planting.value as planting_date,
                nd_geolocation.nd_geolocation_id as location_id
            FROM project
            LEFT JOIN projectprop projectprop_planting 
                ON project.project_id = projectprop_planting.project_id 
                AND projectprop_planting.type_id = (SELECT cvterm_id FROM cvterm WHERE name = 'project_planting_date')
            LEFT JOIN nd_experiment_project ON project.project_id = nd_experiment_project.project_id
            LEFT JOIN nd_experiment ON nd_experiment_project.nd_experiment_id = nd_experiment.nd_experiment_id
            LEFT JOIN nd_geolocation ON nd_experiment.nd_geolocation_id = nd_geolocation.nd_geolocation_id
            WHERE project.project_id = ?
            LIMIT 1
        });
        $sth_trial->execute($trial_id);
        my $trial_info = $sth_trial->fetchrow_hashref();
        
        my $planting_date_raw = $trial_info->{planting_date};
        my $location_id = $trial_info->{location_id};
        
        # Parse planting date from various formats
        # Could be: simple date, timestamp, or JSON array like {"2025-04-15T00:00:00",...}
        my $planting_date;
        if ($planting_date_raw) {
            # Extract first date from JSON-like array format
            if ($planting_date_raw =~ /\{?"?(\d{4}-\d{2}-\d{2})/) {
                $planting_date = $1;
            } elsif ($planting_date_raw =~ /^(\d{4}-\d{2}-\d{2})/) {
                $planting_date = $1;
            }
        }
        
        unless ($planting_date && $location_id) {
            $c->stash->{rest} = { error => "Trial missing planting date (got: $planting_date_raw) or location" };
            return;
        }
        
        # Get the PHENOTYPING experiment for each stock 
        my $placeholders = join(',', ('?') x scalar(@$accession_ids));
        my $sth_plots = $dbh->prepare(qq{
            SELECT s.stock_id, nde.nd_experiment_id
            FROM stock s
            JOIN nd_experiment_stock nes ON s.stock_id = nes.stock_id
            JOIN nd_experiment nde ON nes.nd_experiment_id = nde.nd_experiment_id
            JOIN cvterm c ON nde.type_id = c.cvterm_id
            WHERE c.name = 'phenotyping_experiment'
            AND s.stock_id IN ($placeholders)
        });
        $sth_plots->execute(@$accession_ids);
        
        my %stock_experiments;
        while (my $row = $sth_plots->fetchrow_hashref) {
            $stock_experiments{$row->{stock_id}} = $row->{nd_experiment_id};
        }
        
        # Get maturity values and auto-detect format from actual data values
        # - If value is numeric and < 200 -> treat as days
        # - If value matches date pattern (yyyymmdd, yymmdd, yyyy-mm-dd) -> convert to days
        my $sth_maturity = $dbh->prepare(qq{
            SELECT 
                nes.stock_id,
                p.value as maturity_value
            FROM phenotype p
            JOIN nd_experiment_phenotype nep ON p.phenotype_id = nep.phenotype_id
            JOIN nd_experiment_stock nes ON nep.nd_experiment_id = nes.nd_experiment_id
            WHERE nes.stock_id IN ($placeholders)
            AND p.cvalue_id = ?
        });
        $sth_maturity->execute(@$accession_ids, $maturity_day_trait_id);
        
        # Parse planting date for date calculations
        use Time::Piece;
        my $planting_tp = Time::Piece->strptime($planting_date, '%Y-%m-%d');
        
        my %plot_maturity_days;
        my @format_errors;
        
        while (my $row = $sth_maturity->fetchrow_hashref) {
            my $val = $row->{maturity_value};
            next unless defined $val && $val ne '';
            
            my $stock_id = $row->{stock_id};
            $val =~ s/^\s+|\s+$//g;  # Trim
            
            # Auto-detect format from value
            if ($val =~ /^(\d+)$/ && $1 < 200) {
                # Numeric value < 200 = treat as days after planting
                $plot_maturity_days{$stock_id} = int($1);
            } elsif ($val =~ /^(\d{4})(\d{2})(\d{2})$/) {
                # yyyymmdd format
                my ($y, $m, $d) = ($1, $2, $3);
                my $mat_date_str = sprintf("%04d-%02d-%02d", $y, $m, $d);
                my $mat_tp = Time::Piece->strptime($mat_date_str, '%Y-%m-%d');
                my $diff_days = int(($mat_tp - $planting_tp) / (24 * 60 * 60));
                $plot_maturity_days{$stock_id} = $diff_days if $diff_days > 0;
            } elsif ($val =~ /^(\d{2})(\d{2})(\d{2})$/) {
                # yymmdd format
                my ($y, $m, $d) = ("20$1", $2, $3);
                my $mat_date_str = sprintf("%04d-%02d-%02d", $y, $m, $d);
                my $mat_tp = Time::Piece->strptime($mat_date_str, '%Y-%m-%d');
                my $diff_days = int(($mat_tp - $planting_tp) / (24 * 60 * 60));
                $plot_maturity_days{$stock_id} = $diff_days if $diff_days > 0;
            } elsif ($val =~ /^(\d{4})-(\d{2})-(\d{2})/) {
                # yyyy-mm-dd format
                my ($y, $m, $d) = ($1, $2, $3);
                my $mat_date_str = sprintf("%04d-%02d-%02d", $y, $m, $d);
                my $mat_tp = Time::Piece->strptime($mat_date_str, '%Y-%m-%d');
                my $diff_days = int(($mat_tp - $planting_tp) / (24 * 60 * 60));
                $plot_maturity_days{$stock_id} = $diff_days if $diff_days > 0;
            } else {
                # Unknown format - skip but track for error reporting
                push @format_errors, { stock_id => $stock_id, value => $val };
            }
        }
        
        # If we have format errors and no successful conversions, report error
        if (@format_errors && scalar(keys %plot_maturity_days) == 0) {
            my $sample = $format_errors[0];
            $c->stash->{rest} = { 
                error => "Unrecognized data format: '$sample->{value}'. Expected either days count (numeric < 200) or date format (yyyymmdd, yymmdd, yyyy-mm-dd).",
                sample_value => $sample->{value},
                stock_id => $sample->{stock_id}
            };
            return;
        }
        
        # Get emergence values for each plot (days after planting when emergence occurred)
        my %plot_emergence_days;
        if ($emergence_trait_id) {
            my $sth_emergence = $dbh->prepare(qq{
                SELECT nes.stock_id, p.value as emergence_value
                FROM phenotype p
                JOIN nd_experiment_phenotype nep ON p.phenotype_id = nep.phenotype_id
                JOIN nd_experiment_stock nes ON nep.nd_experiment_id = nes.nd_experiment_id
                WHERE nes.stock_id IN ($placeholders)
                AND p.cvalue_id = ?
            });
            $sth_emergence->execute(@$accession_ids, $emergence_trait_id);
            
            while (my $row = $sth_emergence->fetchrow_hashref) {
                my $val = $row->{emergence_value};
                next unless defined $val && $val ne '';
                
                my $stock_id = $row->{stock_id};
                $val =~ s/^\s+|\s+$//g;  # Trim
                
                # Auto-detect format from value (same logic as maturity)
                if ($val =~ /^(\d+)$/ && $1 < 200) {
                    # Numeric value < 200 = treat as days after planting
                    $plot_emergence_days{$stock_id} = int($1);
                } elsif ($val =~ /^(\d{4})(\d{2})(\d{2})$/) {
                    # yyyymmdd format
                    my ($y, $m, $d) = ($1, $2, $3);
                    my $em_date_str = sprintf("%04d-%02d-%02d", $y, $m, $d);
                    my $em_tp = Time::Piece->strptime($em_date_str, '%Y-%m-%d');
                    my $diff_days = int(($em_tp - $planting_tp) / (24 * 60 * 60));
                    $plot_emergence_days{$stock_id} = $diff_days if $diff_days > 0;
                } elsif ($val =~ /^(\d{2})(\d{2})(\d{2})$/) {
                    # yymmdd format
                    my ($y, $m, $d) = ("20$1", $2, $3);
                    my $em_date_str = sprintf("%04d-%02d-%02d", $y, $m, $d);
                    my $em_tp = Time::Piece->strptime($em_date_str, '%Y-%m-%d');
                    my $diff_days = int(($em_tp - $planting_tp) / (24 * 60 * 60));
                    $plot_emergence_days{$stock_id} = $diff_days if $diff_days > 0;
                } elsif ($val =~ /^(\d{4})-(\d{2})-(\d{2})/) {
                    # yyyy-mm-dd format
                    my ($y, $m, $d) = ($1, $2, $3);
                    my $em_date_str = sprintf("%04d-%02d-%02d", $y, $m, $d);
                    my $em_tp = Time::Piece->strptime($em_date_str, '%Y-%m-%d');
                    my $diff_days = int(($em_tp - $planting_tp) / (24 * 60 * 60));
                    $plot_emergence_days{$stock_id} = $diff_days if $diff_days > 0;
                }
                # Unknown format values are simply skipped
            }
        }
        
        # Get location lat/lon
        my $sth_loc = $dbh->prepare(q{
            SELECT latitude, longitude FROM nd_geolocation WHERE nd_geolocation_id = ?
        });
        $sth_loc->execute($location_id);
        my ($lat, $lon) = $sth_loc->fetchrow_array();
        
        unless ($lat && $lon) {
            $c->stash->{rest} = { error => "Location has no coordinates" };
            return;
        }
        
        # Get weather data for entire possible period (planting to furthest maturity)
        my @all_maturity_days = values %plot_maturity_days;
        my $max_days = @all_maturity_days ? (sort { $b <=> $a } @all_maturity_days)[0] : 100;
        
        # Calculate end date as planting + max_days
        my $end_tp = $planting_tp + ($max_days * 24 * 60 * 60);
        my $full_end_date = $end_tp->strftime('%Y-%m-%d');
        
        # Fetch weather data once for full period
        my @weather_data = @{$self->_get_cached_weather($c, $location_id, $planting_date, $full_end_date)};
        
        # Build day index for quick lookup
        my %weather_by_day;
        my $day_num = 0;
        foreach my $day (@weather_data) {
            $day_num++;
            $weather_by_day{$day_num} = $day;
        }
        
        # Prepared statements
        # Check for existing phenotype for this stock+trait
        my $sth_check_existing = $dbh->prepare(q{
            SELECT p.phenotype_id 
            FROM phenotype p
            JOIN nd_experiment_phenotype nep ON p.phenotype_id = nep.phenotype_id
            JOIN nd_experiment_stock nes ON nep.nd_experiment_id = nes.nd_experiment_id
            WHERE nes.stock_id = ?
            AND p.cvalue_id = ?
            LIMIT 1
        });
        
        # Update existing phenotype
        my $sth_update_pheno = $dbh->prepare(q{
            UPDATE phenotype 
            SET value = ?, collect_date = ?, operator = 'GDD Calculator (updated)'
            WHERE phenotype_id = ?
        });
        
        my $sth_insert_pheno = $dbh->prepare(q{
            INSERT INTO phenotype (uniquename, observable_id, cvalue_id, value, collect_date, operator)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING phenotype_id
        });
        
        my $sth_link_exp = $dbh->prepare(q{
            INSERT INTO nd_experiment_phenotype (nd_experiment_id, phenotype_id)
            VALUES (?, ?)
            ON CONFLICT DO NOTHING
        });
        
        my $created = 0;
        my @results;
        
        foreach my $stock_id (@$accession_ids) {
            my $exp_id = $stock_experiments{$stock_id};
            next unless $exp_id;
            
            # Get this plot's maturity days
            my $maturity_days = $plot_maturity_days{$stock_id};
            
            unless ($maturity_days && $maturity_days > 0) {
                # Skip plots without maturity data
                next;
            }
            
            # Get emergence days for this plot
            my $start_day;
            my $emergence_days = $plot_emergence_days{$stock_id};
            
            if ($emergence_days && $emergence_days > 0 && $emergence_days < $maturity_days) {
                # Use emergence as start day
                $start_day = $emergence_days;
            } elsif ($use_planting_fallback) {
                # Fallback to planting date + user-specified emergence days
                $start_day = $fallback_emergence_days;
            } else {
                # No emergence data and no fallback - skip this plot
                next;
            }
            
            # Calculate GDD/CHU for this specific plot (from emergence to maturity)
            my ($plot_gdd, $plot_chu) = (0, 0);
            my $base_temp = 10;
            
            for (my $day = $start_day; $day <= $maturity_days; $day++) {
                my $weather = $weather_by_day{$day};
                next unless $weather && defined($weather->{tmax}) && defined($weather->{tmin});
                
                my $tmax = $weather->{tmax} + 0;
                my $tmin = $weather->{tmin} + 0;
                
                # GDD calculation
                my $tavg = ($tmax + $tmin) / 2;
                my $gdd = $tavg > $base_temp ? $tavg - $base_temp : 0;
                $plot_gdd += $gdd;
                
                # CHU calculation (Ontario method)
                my $chu_max = $tmax > 10 ? 3.33 * ($tmax - 10) - 0.084 * (($tmax - 10) ** 2) : 0;
                my $chu_min = $tmin > 4.4 ? 1.8 * ($tmin - 4.4) : 0;
                my $chu_day = ($chu_max + $chu_min) / 2;
                $chu_day = 0 if $chu_day < 0;
                $plot_chu += $chu_day;
            }
            
            # Calculate collect_date for this plot
            my $collect_tp = $planting_tp + ($maturity_days * 24 * 60 * 60);
            my $collect_date = $collect_tp->strftime('%Y-%m-%d');
            
            my $timestamp = time();
            
            # Insert or Update GDD phenotype
            my $gdd_val = sprintf("%.1f", $plot_gdd);
            
            # Check if GDD phenotype already exists for this stock
            $sth_check_existing->execute($stock_id, $gdd_trait_id);
            my ($existing_gdd_id) = $sth_check_existing->fetchrow_array();
            
            if ($existing_gdd_id) {
                # Update existing
                $sth_update_pheno->execute($gdd_val, $collect_date, $existing_gdd_id);
                $created++;
            } else {
                # Insert new
                my $gdd_uniquename = "gdd_stock_${stock_id}_trial_${trial_id}_${timestamp}";
                $sth_insert_pheno->execute($gdd_uniquename, $gdd_trait_id, $gdd_trait_id, $gdd_val, $collect_date, 'GDD Calculator');
                my ($gdd_pheno_id) = $sth_insert_pheno->fetchrow_array();
                if ($gdd_pheno_id) {
                    $sth_link_exp->execute($exp_id, $gdd_pheno_id);
                    $created++ if $sth_link_exp->rows > 0;
                }
            }
            
            # Insert or Update CHU phenotype
            my $chu_val = sprintf("%.1f", $plot_chu);
            
            # Check if CHU phenotype already exists for this stock
            $sth_check_existing->execute($stock_id, $chu_trait_id);
            my ($existing_chu_id) = $sth_check_existing->fetchrow_array();
            
            if ($existing_chu_id) {
                # Update existing
                $sth_update_pheno->execute($chu_val, $collect_date, $existing_chu_id);
                $created++;
            } else {
                # Insert new
                my $chu_uniquename = "chu_stock_${stock_id}_trial_${trial_id}_${timestamp}";
                $sth_insert_pheno->execute($chu_uniquename, $chu_trait_id, $chu_trait_id, $chu_val, $collect_date, 'GDD Calculator');
                my ($chu_pheno_id) = $sth_insert_pheno->fetchrow_array();
                if ($chu_pheno_id) {
                    $sth_link_exp->execute($exp_id, $chu_pheno_id);
                    $created++ if $sth_link_exp->rows > 0;
                }
            }
            
            push @results, {
                stock_id => $stock_id,
                maturity_days => $maturity_days,
                gdd => $gdd_val,
                chu => $chu_val,
            };
        }
        
        $c->stash->{rest} = {
            success => 1,
            created => $created,
            crop => $crop,
            gdd_trait_id => $gdd_trait_id,
            chu_trait_id => $chu_trait_id,
            message => "Created $created GDD/CHU phenotypes for $crop (per-plot calculation)",
            results => \@results
        };
        
    } catch {
        $c->stash->{rest} = { error => "Failed to store phenotypes: $_" };
    };
}

# ============================================================================
# HELPER SUBROUTINES
# ============================================================================

# Parse a maturity value and return days from planting
# Arguments: $value, $planting_date (YYYY-MM-DD)
# Returns: (days, error_message) - days is undef on error
sub _parse_maturity_value {
    my ($value, $planting_date) = @_;
    
    return (undef, "Empty value") unless defined $value && $value ne '';
    
    $value =~ s/^\s+|\s+$//g;  # Trim whitespace
    
    # Numeric value < 200 = treat as days after planting
    if ($value =~ /^(\d+)$/ && $1 < 200) {
        return (int($1), undef);
    }
    
    # Date formats require planting_date
    unless ($planting_date) {
        return (undef, "Planting date required for date conversion");
    }
    
    require Time::Piece;
    my $planting_tp = Time::Piece->strptime($planting_date, '%Y-%m-%d');
    
    my ($y, $m, $d);
    
    # yyyymmdd format
    if ($value =~ /^(\d{4})(\d{2})(\d{2})$/) {
        ($y, $m, $d) = ($1, $2, $3);
    }
    # yymmdd format  
    elsif ($value =~ /^(\d{2})(\d{2})(\d{2})$/) {
        ($y, $m, $d) = ("20$1", $2, $3);
    }
    # yyyy-mm-dd format
    elsif ($value =~ /^(\d{4})-(\d{2})-(\d{2})/) {
        ($y, $m, $d) = ($1, $2, $3);
    }
    else {
        return (undef, "Unrecognized format: '$value'. Expected days (<200) or date (yyyymmdd/yymmdd/yyyy-mm-dd)");
    }
    
    my $mat_date_str = sprintf("%04d-%02d-%02d", $y, $m, $d);
    my $mat_tp = Time::Piece->strptime($mat_date_str, '%Y-%m-%d');
    my $diff_days = int(($mat_tp - $planting_tp) / (24 * 60 * 60));
    
    return ($diff_days > 0 ? $diff_days : undef, $diff_days <= 0 ? "Maturity date before planting" : undef);
}

# Detect format from trait name for UI badges
# Returns: 'day', 'date', or 'unknown'
sub _detect_trait_format_from_name {
    my ($trait_name) = @_;
    
    if ($trait_name =~ /-(?: |)day(?:s)?$/i || $trait_name =~ /time - day/i) {
        return 'day';
    }
    elsif ($trait_name =~ /yyyymmdd/i || $trait_name =~ /date/i) {
        return 'date';
    }
    return 'unknown';
}

1;
